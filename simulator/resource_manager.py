from re import T
from flask.helpers import make_response
import requests
import pickle
import databus as dbs
from flask import Flask, jsonify
import json
import pymongo
import argparse
import threading
from threading import RLock
import pandas as pd
from flask import request

app = Flask(__name__)


def _find(query, one=False):
    with lock:
        if one:
            return resource_pool.find_one(query)
        else:
            return pd.DataFrame(list(resource_pool.find(query)))

def _update(filter, operations, one=True):
    with lock:
        if one:
            result = resource_pool.update_one(filter, operations)
        else:
            result = resource_pool.update_many(filter, operations)
    return result.modified_count

def _insert(data, one=True):
    with lock:
        if one:
            resource_pool.insert_one(data)
        else:
            resource_pool.insert_many(data)


def preempt_nodes(request_data):
    osg_nodes = _find({"$and": [{"node_type": request_data['node_type']}, {"pool": "osg"}]})
    osg_nodes.sort_values(by=['inuse_cpus'], inplace=True)
    if osg_nodes.shape[0] > request_data['node_cnt']:
        osg_nodes = osg_nodes.iloc[:int(request_data['node_cnt'])]
    payload = {"terminate_nodes": osg_nodes['HOST_NAME (PHYSICAL)'].to_list()}
    _update(
        {"HOST_NAME (PHYSICAL)": {"$in": osg_nodes['HOST_NAME (PHYSICAL)'].to_list()}}, 
        {"$set": {"status": "inuse", "pool": "chameleon", "inuse_cpus": 0, "inuse_memory": 0, "backfill": []}},
        one=False
    )
    dbs.emit_msg("osg_jobs_exchange", "terminate_osg_job", json.dumps(payload), rm_channel)


@app.route('/acquire_nodes', methods=['POST'])
def acquire_nodes():
    request_data = request.get_json()
    if request_data['pool'] == 'chameleon':
        while True:
            free_nodes = _find({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}, {"pool": "chameleon"}]}).iloc[:int(request_data['node_cnt'])]
            if free_nodes.shape[0] < request_data['node_cnt']:
                temp = request_data.copy()
                temp['node_cnt'] = request_data['node_cnt'] - free_nodes.shape[0]
                preempt_nodes(temp)
            else:
                assert free_nodes.shape[0] >= request_data['node_cnt']
                result = _update(filter={"HOST_NAME (PHYSICAL)": {"$in": free_nodes['HOST_NAME (PHYSICAL)'].to_list()}}, operations={"$set": {"status": "inuse"}}, one=False)
                assert result >= request_data['node_cnt']
                break
    elif request_data['pool'] == 'osg':
        result = _update(filter={"$and": [{"status": "free"}, {"pool": "chameleon"}]}, operations={"$set": {"status": "inuse", "pool": "osg"}})
        if result == 0:
            return 'no node is available for osg', 202
    return 'OK', 200


@app.route('/release_nodes', methods=['POST'])
def release_nodes():
    request_data = request.get_json()
    inuse_nodes = _find({"$and": [
        {"node_type": request_data['node_type']}, 
        {"pool": "chameleon"},
        {"status": "inuse"}]}).iloc[:int(request_data['node_cnt'])]
    if inuse_nodes.shape[0] < request_data['node_cnt']:
        return 'release node %d < inuse nodes %d' % (request_data['node_cnt'], inuse_nodes.shape[0]), 202
    else:
        result = _update(filter={"HOST_NAME (PHYSICAL)": {"$in": inuse_nodes['HOST_NAME (PHYSICAL)'].to_list()}}, operations= {"$set": {"status": "free", "pool": 'chameleon'}}, one=False)
        if result == request_data['node_cnt']:
            return 'OK', 200
        else:
            return 'error: release', 202


@app.route('/find', methods=['POST'])
def find():
    results = _find(request.get_json())
    if '_id' in results.columns:
        del results['_id']
    if results.shape[0] > 0:
        return make_response(jsonify(results.to_dict(orient="records")), 200)
    else:
        return 'Null', 202


@app.route('/update', methods=['POST'])
def update():
    request_data = request.get_json()
    result = _update(**request_data)
    if result >= 1:
        return 'OK', 200
    else:
        return 'Null', 202


def process_machine_event(ch, method, properties, body):
    if properties.headers['key'] != 'machine_event':
        return
    machine_event = json.loads(body)
    machine_id = machine_event['HOST_NAME (PHYSICAL)']
    machine = _find({"HOST_NAME (PHYSICAL)": machine_id}, one=True)
    if machine_event['EVENT'] in ['ENABLE', 'UPDATE']:
        if not machine:
            # parse node type
            properties = machine_event['PROPERTIES']
            properties = properties.replace('\'', '\"')
            properties = properties.replace('None', '\"None\"')
            if 'node_type' in properties:
                machine_event['node_type'] = json.loads(properties)['node_type']
            else:
                machine_event['node_type'] = None
            machine_event.update({
                "status": "free",
                "pool": "chameleon",
                "cpus": hardware_profile[machine_event['node_type']]['CPU'],
                "inuse_cpus": 0,
                "memory": hardware_profile[machine_event['node_type']]['RAM'],
                "inuse_memory": 0,
                "backfill": []
            })
            _insert(machine_event)
    elif machine_event['EVENT'] == 'DISABLE':
        if machine:
            machine_event.update({
                "pool": "chameleon",
                "status": "inactive",
                "inuse_cpus": 0,
                "inuse_memory": 0,
                "backfill": []
            })
            assert _update(filter={"HOST_NAME (PHYSICAL)": machine_id}, operations={"$set": machine_event}) == 1



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost', help='host IP for running the resource manager')
    parser.add_argument('--mongo', type=str, default='mongodb://chi-sim:chi-sim@127.0.0.1:27017', help='MongoDB connection URL')
    args = parser.parse_args()

    mongo_client = pymongo.MongoClient(args.mongo)
    mongo_client.drop_database('ChameleonSimulator')
    db = mongo_client['ChameleonSimulator']
    resource_pool = db['resource_pool']
    with open('hardware.json') as f:
        hardware_profile = json.load(f)
    dbs_connection = dbs.init_connection()
    rm_channel = dbs_connection.channel()

    lock = RLock()
    # listen machine events
    thread1 = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('machine_events_exchange', 'machine_events_queue', 'machine_event', process_machine_event))
    thread1.start()
    app.run(host=args.host, port=5000, debug=True)
    thread1.join()
import databus as dbs
from flask import Flask
import json
import pymongo
import argparse
import threading
import pandas as pd
from flask import request

app = Flask(__name__)


def preempt_nodes(request_data):
    osg_nodes = resource_pool.find({"$and": [{"node_type": request_data['node_type']}, {"pool": "osg"}]})
    osg_nodes = pd.DataFrame(list(osg_nodes))
    if osg_nodes.shape[0] > request_data['node_cnt']:
        # select nodes that have more available cpus
        osg_nodes.sort_values(by=['inuse_cpus'], inplace=True)
        osg_nodes = osg_nodes.iloc[:int(request_data['node_cnt'])]
        payload = {"terminate_nodes": osg_nodes['HOST_NAME (PHYSICAL)'].to_list()}
        dbs.emit_msg("osg_jobs_exchange", "terminate_osg_job", json.dumps(payload), rm_channel)
        resource_pool.update_many(
            {"HOST_NAME (PHYSICAL)": {"$in": osg_nodes['HOST_NAME (PHYSICAL)'].to_list()}},
            {"$set": {"status": "inuse", "pool": "chameleon", "inuse_cpus": 0, "inuse_memory": 0}})
        return True
    return False


@app.route('/acquire_nodes', methods=['POST'])
def acquire_nodes():
    request_data = request.get_json()
    if request_data['pool'] == 'chameleon':
        while True:
            nodes = resource_pool.find({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}, {"pool": "chameleon"}]}).limit(int(request_data['node_cnt']))
            if len(list(nodes)) < request_data['node_cnt']:
                temp = request_data.copy()
                temp['node_cnt'] = request_data['node_cnt'] - len(list(nodes))
                if preempt_nodes(temp):
                    break
            else:
                break
        nodes = resource_pool.find({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}, {"pool": "chameleon"}]}).limit(int(request_data['node_cnt']))
        free_nodes = pd.DataFrame(list(nodes))
        assert free_nodes.shape[0] >= request_data['node_cnt']
        resource_pool.update_many(
            {"HOST_NAME (PHYSICAL)": {"$in": free_nodes['HOST_NAME (PHYSICAL)'].to_list()}},
            {"$set": {"status": "inuse"}}
        )
    elif request_data['pool'] == 'osg':
        ch_node = resource_pool.find_one({"$and": [{"status": "free"}, {"pool": "chameleon"}]})
        if ch_node:
            resource_pool.update_one(
                {"HOST_NAME (PHYSICAL)": ch_node['HOST_NAME (PHYSICAL)']},
                {"$set": {"status": "inuse", "pool": "osg"}}
            )
    return 'OK', 200
    


@app.route('/release_nodes', methods=['POST'])
def release_nodes():
    request_data = request.get_json()
    nodes = resource_pool.find({"$and": [
        {"node_type": request_data['node_type']}, 
        {"pool": "chameleon"},
        {"status": "inuse"}]}).limit(int(request_data['node_cnt']))
    inuse_nodes = pd.DataFrame(list(nodes))
    if inuse_nodes.shape[0] < request_data['node_cnt']:
        return 'release node %d < inuse nodes %d' % (request_data['node_cnt'], inuse_nodes.shape[0]), 202
    else:
        resource_pool.update_many(
            {"HOST_NAME (PHYSICAL)": {"$in": inuse_nodes['HOST_NAME (PHYSICAL)'].to_list()}},
            {"$set": {"status": "free", "pool": 'chameleon'}}
        )
        return 'OK', 200


def process_machine_event(ch, method, properties, body):
    if properties.headers['key'] != 'machine_event':
        return
    machine_event = json.loads(body)
    machine_id = machine_event['HOST_NAME (PHYSICAL)']
    machine = resource_pool.find_one({"HOST_NAME (PHYSICAL)": machine_id})
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
            resource_pool.insert_one(machine_event)
    elif machine_event['EVENT'] == 'DISABLE':
        if machine:
            machine_event.update({
                "pool": "chameleon",
                "status": "inactive",
                "inuse_cpus": 0,
                "inuse_memory": 0,
                "backfill": []
            })
            resource_pool.update_one({"HOST_NAME (PHYSICAL)": machine_id}, {"$set": machine_event})


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

    # listen machine events
    thread1 = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('machine_events_exchange', 'machine_events_queue', 'machine_event', process_machine_event))
    thread1.start()
    app.run(host=args.host, port=5000, debug=False)
    thread1.join()
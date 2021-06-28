import databus as dbs
from flask import Flask
import json
import pymongo
import argparse
import threading
import pickle
import pandas as pd
from flask import request

app = Flask(__name__)


def assign(node_type, node_cnt, pool):
    nodes = resource_pool.find({"$and": [{"node_type": node_type}, {"status": "free"}]}).limit(node_cnt)
    if nodes.count() < node_cnt:
        return False, node_cnt - nodes.count()
    else:
        node_ids = [node['_id'] for node in nodes]
        resource_pool.update_many(
            {"_id": {"$in": node_ids}},
            {"$set": {"status": "inuse", "pool": pool}}
        )
        return True, 0


def release(node_type, node_cnt):
    nodes = resource_pool.find({"$and": [{"node_type": node_type}, {"status": "inuse"}]})
    if nodes.count() < node_cnt:
        return False, nodes.count()
    else:
        node_ids = [node['_id'] for node in nodes]
        resource_pool.update_many(
            {"_id": {"$in": node_ids}},
            {"$set": {"status": "free", "pool": 'Chameleon'}}
        )
        return True, 0


@app.route('/get_free_nodes', method='GET')
def get_free_nodes():
    node_type = request.args.get('node_type')
    free_nodes = resource_pool.find({"$and": [{"node_type": node_type}, {"status": "free"}]}).count()
    return free_nodes


# TODO: in-advance, on_demand_predict, on_demand_makeup均通过次acquire函数请求资源
@app.route('/acquire_nodes', method='POST')
def acquire_nodes():
    request_data = request.get_json()
    results = assign(request_data['node_type'], request_data['node_count'], request_data['pool'])
    if results[0]:
        return 200, 'OK'
    else:
        return 201, str(results[1])


@app.route('/release_nodes', method='POST')
def release_nodes():
    request_data = request.get_json()
    results = release(request_data['node_type'], request_data['node_count'])
    if results:
        return 200, 'OK'
    else:
        return 403, 'release node %d < available nodes %d' % (request_data['node_count'], results[1])


@app.route('/preempt_nodes', method='POST')
def preempt_nodes():
    request_data = request.get_json()
    resource_pool.update_many({"HOST_NAME (PHYSICAL)": {"$in": request_data['preempt_nodes']}},
                              {"$set": {"status": "inuse", "pool": "Chameleon"}})
    resource_pool.update_many({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}]},
                              {"$set": {"status": "inuse", "pool": "Chameleon"}})


def process_machine_event(ch, method, properties, body):
    machine_event = pickle.loads(body)
    machine_id = machine_event['HOST_NAME (PHYSICAL)']
    machine = resource_pool.find_one({"HOST_NAME (PHYSICAL)": machine_id})

    if machine_event['EVENT'] in ['ENABLE']:
        if not machine:
            # parse node type
            node_type = machine_event['PROPERTIES']
            node_type = node_type.replace('\'', '\"')
            node_type = node_type.replace('None', '\"None\"')
            if 'node_type' in node_type:
                machine_event['node_type'] = json.loads(node_type)['node_type']
            else:
                machine_event['node_type'] = None
            machine_event['status'] = 'free'
            machine_event['pool'] = 'Chameleon'
            resource_pool.insert_one(machine_event)
    elif machine_event['EVENT'] in ['DISABLE']:
        if machine:
            if machine['pool'] == 'OSG':
                # TODO: machine is disabled by the Chameleon Operator
                pass
            resource_pool.update_one({"HOST_NAME (PHYSICAL)": machine_id},
                                     {"$set": {"status": "inactive", "pool": "Chameleon"}})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, help='host IP for running the resource manager')
    parser.add_argument('--mongo', type=str, help='MongoDB connection URL')
    args = parser.parse_args()

    mongo_client = pymongo.MongoClient(args.mongo)
    db = mongo_client['ChameleonSimulator']
    resource_pool = db['resource_pool']
    app.run()

    dbs_connection = dbs.init_connection()
    resource_manager_channel = dbs_connection.channel()
    # listen machine events
    lme = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('resource_manager', 'machine_events', 'raw_event', process_machine_event, dbs_connection))

    lme.start()
    app.run(host=args.host, port=5000, debug=False)
    lme.join()
    dbs_connection.close()

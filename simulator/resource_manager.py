import databus as dbs
from flask import Flask
import json
import pymongo
import argparse
import threading
import pandas as pd
from flask import request

app = Flask(__name__)


@app.route('/get_free_nodes', methods=['GET'])
def get_free_nodes():
    node_type = request.args.get('node_type')
    free_nodes = resource_pool.find({"$and": [{"node_type": node_type}, {"status": "free"}]}).count()
    return str(free_nodes), 200


def preempt_nodes():
    # TODO: 此处应有preemption policy决定
    request_data = None
    resource_pool.update_many({"HOST_NAME (PHYSICAL)": {"$in": request_data['preempt_nodes']}},
                              {"$set": {"status": "inuse", "pool": "chameleon"}})
    resource_pool.update_many({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}]},
                              {"$set": {"status": "inuse", "pool": "chameleon"}})


@app.route('/acquire_nodes', methods=['POST'])
def acquire_nodes():
    request_data = request.get_json()
    nodes = resource_pool.find({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}]}).limit(int(request_data['node_cnt']))
    if nodes.count() < request_data['node_cnt']:
        preempt_nodes()
    nodes = resource_pool.find({"$and": [{"node_type": request_data['node_type']}, {"status": "free"}]}).limit(int(request_data['node_cnt']))
    node_ids = [node['HOST_NAME (PHYSICAL)'] for node in nodes]
    resource_pool.update_many(
        {"HOST_NAME (PHYSICAL)": {"$in": node_ids}},
        {"$set": {"status": "inuse", "pool": request_data['pool']}}
    )
    return 'OK', 200


@app.route('/release_nodes', methods=['POST'])
def release_nodes():
    request_data = request.get_json()
    nodes = resource_pool.find({"$and": [{"node_type": request_data['node_type']}, {"status": "inuse"}]})
    if nodes.count() < request_data['node_cnt']:
        return 'release node %d < inuse nodes %d' % (request_data['node_cnt'], nodes.count()), 403
    else:
        node_ids = [node['HOST_NAME (PHYSICAL)'] for node in nodes]
        resource_pool.update_many(
            {"HOST_NAME (PHYSICAL)": {"$in": node_ids}},
            {"$set": {"status": "free", "pool": 'chameleon'}}
        )
        return 'OK', 200


@app.route('/submit_osg_job', methods=['POST'])
def submit_osg_job():
    osg_job = request.get_json()
    nodes = resource_pool.find({"pool": "osg"})
    nodes = pd.DataFrame(list(nodes))
    nodes = nodes[(nodes['cpus'] - nodes['inuse_cpus'] > osg_job['CpusProvisioned']) & (nodes['memory'] - nodes['inuse_memory'] > osg_job['MemoryProvisioned'])]
    first_fit_node = nodes.iloc[0]  # first fit node
    osg_job['Machine'] = first_fit_node['HOST_NAME (PHYSICAL)'] 
    osg_job['JobSimStatus'] = 'running'
    resource_pool.update_one(
        {"HOST_NAME (PHYSICAL)": osg_job['Machine']},
        {"$set": {"inuse_cpus": first_fit_node['inuse_cpus'] + osg_job['CpusProvisioned'], "inuse_memory": first_fit_node['inuse_memory'] + osg_job['MemoryProvisioned']}}
    )
    return osg_job, 200


def process_machine_event(ch, method, properties, body):
    global resource_pool
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
            machine_event['status'] = 'free'
            machine_event['pool'] = 'chameleon'
            resource_pool.insert_one(machine_event)
    elif machine_event['EVENT'] == 'DISABLE':
        if machine:
            if machine['pool'] == 'OSG':
                # TODO: machine is disabled by the chameleon Operator
                pass
            machine_event['status'] = 'inactive'
            machine_event['pool'] = 'chameleon'
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
    
    # listen machine events
    thread1 = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('machine_events_exchange', 'machine_events_queue', 'machine_event', process_machine_event))
    thread1.start()
    app.run(host=args.host, port=5000, debug=False)
    thread1.join()
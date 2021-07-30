from datetime import datetime
import logging
from re import T
from flask.helpers import make_response
from flask.scaffold import F
from numpy import e
from pika.spec import NOT_IMPLEMENTED
from pymongo import database
import requests
from utils import get_influxdb_info, get_logger, get_mongo_url, load_config
import databus as dbs
from flask import Flask, jsonify
import json
import pymongo
from influxdb import InfluxDBClient
import argparse
import threading
from threading import RLock
import pandas as pd
import time
from flask import request
from monitor import Monitor
import OsgOverhead
import pika
from sklearn.utils import shuffle


OSG_LIMIT = 1  # the maximum portion of nodes that can be allocated to OSG

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True

logger = get_logger(__name__)
# logger.disabled = True
monitor = Monitor()


@app.route('/acquire_nodes', methods=['POST'])
def acquire_nodes():
    global rm_channel, dbs_connection
    if config['simulation']['enable_monitor']:
        monitor.measure_rsrc()
    request_data = request.get_json()
    if request_data['pool'] == 'chameleon':
        ch_node_cnt = 0
        osg_node_cnt = 0
        if request_data['node_cnt'] <= 0:
            return "Fail", 202

        # find nodes from chameleon pool
        ch_nodes = resource_pool.aggregate([
            {"$match": {"node_type": request_data['node_type'], "status": "free", "pool": "chameleon"}},
            {"$project": {
                "HOST_NAME (PHYSICAL)": 1,
                "cpus": 1,
                "memory": 1
            }},
            {"$limit": int(request_data['node_cnt'])}
        ])
        ch_nodes = pd.DataFrame(list(ch_nodes))
        ch_node_cnt = ch_nodes.shape[0]
        
        # find nodes from osg pool
        if ch_node_cnt < request_data['node_cnt']:
            preempt_time = datetime.now().timestamp()
            osg_nodes = resource_pool.aggregate([
                {"$match": {"node_type": request_data['node_type'], "pool": "osg"}},
                {"$project": {
                    "HOST_NAME (PHYSICAL)": 1, "backfill": 1, 
                    'free_cpus': 1, "cpus": 1, "memory": 1, "ready_for_osg": 1}}
            ])
            osg_nodes = pd.DataFrame(list(osg_nodes))
            if osg_nodes.shape[0] > 0:

                # Term policy 1: random
                # osg_nodes = shuffle(osg_nodes) 

                # Term policy 2: least use resources
                # osg_nodes.sort_values(by=['free_cpus'], inplace=True, ascending=False)

                # Term policy 3: most-recent deployed
                # osg_nodes.sort_values(by=['ready_for_osg'], inplace=True, ascending=False)

                # Term policy 4: smallest resubmission
                resubmissions = []
                for bf in osg_nodes['backfill']:
                    count = 0
                    for job in bf:
                        count += job['ResubmitCount']
                    resubmissions.append(count)
                osg_nodes['resubmits'] = resubmissions
                osg_nodes.sort_values(by=['resubmits'], inplace=True)


                osg_nodes = osg_nodes.iloc[:int(request_data['node_cnt'] - ch_node_cnt)]
                osg_node_cnt = osg_nodes.shape[0]

        if ch_node_cnt + osg_node_cnt < request_data['node_cnt']:
            logger.info('chameleon: available_nodes %d < acquire_nodes %d' % (ch_node_cnt + osg_node_cnt, request_data['node_cnt']))
            return 'Fail', 202

        # notify OSG
        if osg_node_cnt > 0:
            osg_jobs = []
            for _, row in osg_nodes.iterrows():
                osg_jobs.extend(row['backfill'])
            if config['simulation']['enable_ml']:
                ahead_time = OsgOverhead.AHEAD_NOTIFY()/scale_ratio
            else:
                ahead_time = 0
            
            channel_closed = False
            while True:
                try:
                    if channel_closed:
                        channel_closed = False
                        dbs_connection = dbs.init_connection()
                        rm_channel = dbs_connection.channel()
                        rm_channel.confirm_delivery()
                    rm_channel.basic_publish(
                        exchange='internal_exchange',
                        routing_key='terminate_osg_job',
                        body=json.dumps({"backfills": osg_jobs, "time": float(preempt_time - ahead_time)}),
                        properties=pika.BasicProperties(delivery_mode=1, headers={'key': 'terminate_osg_job'}),
                        mandatory=True
                    )
                    break
                except:
                    logger.debug("reopening channel")
                    channel_closed = True
            logger.info('osg --> chameleon: %d' % osg_node_cnt)

        # update nodes
        machines = []
        if ch_node_cnt > 0:
            machines.extend(ch_nodes['HOST_NAME (PHYSICAL)'].to_list())
        if osg_node_cnt > 0:
            machines.extend(osg_nodes['HOST_NAME (PHYSICAL)'].to_list())
        
        if ch_nodes.shape[0] > 0:
            all_cpus = int(ch_nodes.iloc[0]['cpus'])
            all_memory = int(ch_nodes.iloc[0]['memory'])
        else:
            all_cpus = int(osg_nodes.iloc[0]['cpus'])
            all_memory = int(osg_nodes.iloc[0]['memory'])

        resource_pool.update_many(
            filter={"HOST_NAME (PHYSICAL)": {"$in": machines}},
            update={"$set": {"status": "inuse", "pool": "chameleon", "free_cpus": all_cpus, "free_memory": all_memory, "backfill": [], "ready_for_osg": 0}}
        )
        return make_response(jsonify({"chameleon_pool": ch_node_cnt, "osg_pool": osg_node_cnt}), 200)
    elif request_data['pool'] == 'osg':
        result = resource_pool.update_one(
            filter={"$and": [{"status": "free"}, {"pool": "chameleon"}]},
            update={"$set": {"status": "inuse", "pool": "osg", "ready_for_osg": datetime.now().timestamp() + OsgOverhead.TOTAL()/scale_ratio}}
        )
        if result.modified_count != 1:
            return 'no node is available for osg', 202
        else:
            logger.info('chameleon --> osg: 1')
            return 'OK', 200


@app.route('/release_nodes', methods=['POST'])
def release_nodes():
    if config['simulation']['enable_monitor']:
        monitor.measure_rsrc()
    with lock:
        global completed_leases
        request_data = request.get_json()
        agg_body = [
            {"$match": { "node_type": request_data['node_type'], "pool": "chameleon", "status": "inuse" }},
            {"$project": {"HOST_NAME (PHYSICAL)": 1}},
            {"$limit": int(request_data['node_cnt'])}
        ]
        inuse_nodes = pd.DataFrame(list(resource_pool.aggregate(agg_body)))
        # print(inuse_nodes.shape[0], request_data['node_cnt'])
        if inuse_nodes.shape[0] == request_data['node_cnt']:
            result = resource_pool.update_many({"HOST_NAME (PHYSICAL)": {"$in": inuse_nodes["HOST_NAME (PHYSICAL)"].to_list()}}, {"$set": {"status": "free", "pool": 'chameleon'}})
            if config['simulation']['enable_monitor']:
                monitor.measure_rsrc()
                monitor.monitor_chameleon(completed_leases)
            if result.modified_count == request_data['node_cnt']:
                completed_leases += 1
                return 'OK', 200
            else:
                logger.error('chameleon: release_nodes %d > inuse_nodes %d' % (result.modified_count, inuse_nodes.shape[0]))
        return 'release node %d < inuse nodes %d' % (request_data['node_cnt'], inuse_nodes.shape[0]), 202


@app.route('/update', methods=['POST'])
def update():
    request_data = request.get_json()
    if request_data['one']:
        result = resource_pool.find_one_and_update(filter=request_data['filter'], update=request_data['operations'], return_document=pymongo.ReturnDocument.AFTER)
        if result:
            del result['_id']
            return make_response(jsonify(result), 200)
    else:
        result = resource_pool.update_many(request_data['filter'], request_data['operations'])
        if result.modified_count >= 1:
            return "OK", 200
    return 'Null', 202


@app.route('/aggregate', methods=['POST'])
def aggregate():
    query_steps = request.get_json()['steps']
    result = resource_pool.aggregate(query_steps)
    result = pd.DataFrame(list(result))
    if result.shape[0] == 0:
        return 'Null',  202
    else:
        del result['_id']
        return make_response(jsonify(result.to_dict(orient="records")), 200)


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
                "free_cpus": hardware_profile[machine_event['node_type']]['CPU'],
                "memory": hardware_profile[machine_event['node_type']]['RAM'],
                "free_memory": hardware_profile[machine_event['node_type']]['RAM'],
                "backfill": [],
                "ready_for_osg": 0
            })
            resource_pool.insert_one(machine_event)
    elif machine_event['EVENT'] == 'DISABLE':
        if machine:
            backfills = machine['backfill']
            if len(backfills) > 0:
                payload = {"backfills": backfills, "time": datetime.now().timestamp() + OsgOverhead.AHEAD_NOTIFY()}
                dbs.emit_msg("internal_exchange", "terminate_osg_job", json.dumps(payload), ch)
            machine_event.update({
                "pool": "chameleon",
                "status": "inactive",
                "free_cpus": hardware_profile[machine_event['node_type']]['CPU'],
                "free_memory": hardware_profile[machine_event['node_type']]['RAM'],
                "backfill": [],
                "ready_for_osg": 0
            })
            resource_pool.update_one(
                filter={"HOST_NAME (PHYSICAL)": machine_id},
                update={"$set": machine_event}
            )


if __name__ == '__main__':
    config = load_config()
    influx_client = InfluxDBClient(**get_influxdb_info(config), database='ChameleonSimulator')

    mongo_client = pymongo.MongoClient(get_mongo_url(config))
    db = mongo_client['ChameleonSimulator']
    resource_pool = db['resource_pool']
    resource_pool.create_index('HOST_NAME (PHYSICAL)')

    scale_ratio = config['simulation']['scale_ratio']
    completed_leases = 0
    with open('hardware.json') as f:
        hardware_profile = json.load(f)
    dbs_connection = dbs.init_connection()
    rm_channel = dbs_connection.channel()
    rm_channel.confirm_delivery()

    lock = RLock()

    # listen machine events
    thread1 = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('machine_events_exchange', 'machine_events_queue', 'machine_event', process_machine_event), daemon=True)
    thread1.start()
    app.run(host=config['framework']['rsrc_mgr']['host'], port=5000, debug=True)
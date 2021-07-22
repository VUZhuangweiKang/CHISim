from datetime import datetime
import logging
from re import T
from flask.helpers import make_response
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


app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True

logger = get_logger(__name__)
monitor = Monitor()

def _update(filter, operations, one=True):
    if one:
        result = resource_pool.update_one(filter, operations)
    else:
        result = resource_pool.update_many(filter, operations)
    return result.modified_count


def preempt_nodes(request_data):
    osg_nodes = resource_pool.aggregate([
        {"$match": {"node_type": request_data['node_type'], "pool": "osg"}},
        {"$limit": int(request_data['node_cnt'])}
    ])
    osg_nodes = pd.DataFrame(list(osg_nodes))
    osg_nodes_cnt = 0
    if osg_nodes.shape[0] > 0:
        osg_nodes.sort_values(by=['inuse_cpus'], inplace=True)        
        osg_jobs = []
        for _, row in osg_nodes.iterrows():
            osg_jobs.extend(row['backfill'])
        dbs.emit_msg("internal_exchange", "terminate_osg_job", json.dumps({"backfills": osg_jobs}), rm_channel)
        time.sleep(OsgOverhead.CLEAN_JOBS()/scale_ratio)
        osg_nodes_cnt = resource_pool.update_many(
            filter={"HOST_NAME (PHYSICAL)": {"$in": osg_nodes['HOST_NAME (PHYSICAL)'].to_list()}},
            update={"$set": {"status": "inuse", "pool": "chameleon", "inuse_cpus": 0, "inuse_memory": 0, "backfill": [], "ready_for_osg": 0}}
        ).modified_count
        logger.info('osg --> chameleon: %d' % osg_nodes_cnt)
    return osg_nodes_cnt


@app.route('/acquire_nodes', methods=['POST'])
def acquire_nodes():
    if config['simulation']['enable_monitor']:
        monitor.measure_rsrc()
    request_data = request.get_json()
    if request_data['pool'] == 'chameleon': 
        ch_nodes_cnt = 0
        osg_nodes_cnt = 0
        ch_nodes = resource_pool.aggregate([
            {"$match": {"node_type": request_data['node_type'], "status": "free", "pool": "chameleon"}},
            {"$project": {"HOST_NAME (PHYSICAL)": 1}},
            {"$limit": int(request_data['node_cnt'])}
        ])
        ch_nodes = pd.DataFrame(list(ch_nodes))
        if ch_nodes.shape[0] > 0:
            ch_nodes_cnt = resource_pool.update_many(
                filter={"HOST_NAME (PHYSICAL)": {"$in": ch_nodes['HOST_NAME (PHYSICAL)'].to_list()}},
                update={"$set": {"status": "inuse"}}
            ).modified_count

        if ch_nodes_cnt < request_data['node_cnt']:
            temp = request_data.copy()
            temp['node_cnt'] -= ch_nodes_cnt
            osg_nodes_cnt = preempt_nodes(temp)

        if ch_nodes_cnt + osg_nodes_cnt < request_data['node_cnt']:
            inuse_nodes = resource_pool.aggregate([
                {"$match": {"node_type": request_data['node_type'], "pool": "chameleon", "status": "inuse"}},
                {"$project": {"HOST_NAME (PHYSICAL)": 1}},
                {"$limit": ch_nodes_cnt + osg_nodes_cnt}
            ])
            inuse_nodes = pd.DataFrame(list(inuse_nodes))
            if inuse_nodes.shape[0] > 0:
                resource_pool.update_many(
                    filter={"$and": [{"HOST_NAME (PHYSICAL)": {"$in": inuse_nodes['HOST_NAME (PHYSICAL)'].to_list()}}, {"status": "inuse"}, {"pool": "chameleon"}]},
                    update={"$set": {"status": "free", "pool": 'chameleon'}}
                )
            logger.info('chameleon: available_nodes %d < acquire_nodes %d' % (ch_nodes_cnt + osg_nodes_cnt, request_data['node_cnt']))
            return 'Fail', 202
        return make_response(jsonify({"chameleon_pool": ch_nodes_cnt, "osg_pool": osg_nodes_cnt}), 200)
    elif request_data['pool'] == 'osg':
        result = resource_pool.update_one(
            filter={"$and": [{"status": "free"}, {"pool": "chameleon"}]},
            update={"$set": {"status": "inuse", "pool": "osg", "ready_for_osg": datetime.now().timestamp() + OsgOverhead.TOTAL()/scale_ratio}}
        ).modified_count
        if result == 0:
            return 'no node is available for osg', 202
        else:
            logger.info('chameleon --> osg: %d' % result)
        return 'OK', 200


@app.route('/release_nodes', methods=['POST'])
def release_nodes():
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
    logger.error('chameleon: release_nodes %d > inuse_nodes %d' % (int(request_data['node_cnt']), inuse_nodes.shape[0]))
    return 'release node %d < inuse nodes %d' % (request_data['node_cnt'], inuse_nodes.shape[0]), 202


@app.route('/update', methods=['POST'])
def update():
    request_data = request.get_json()
    result = _update(**request_data)
    if result >= 1:
        return 'OK', 200
    else:
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
                "inuse_cpus": 0,
                "memory": hardware_profile[machine_event['node_type']]['RAM'],
                "inuse_memory": 0,
                "backfill": [],
                "ready_for_osg": 0
            })
            resource_pool.insert_one(machine_event)
    elif machine_event['EVENT'] == 'DISABLE':
        if machine:
            backfills = machine['backfill']
            if len(backfills) > 0:
                payload = {"backfills": backfills}
                dbs.emit_msg("internal_exchange", "terminate_osg_job", json.dumps(payload), ch)
            machine_event.update({
                "pool": "chameleon",
                "status": "inactive",
                "inuse_cpus": 0,
                "inuse_memory": 0,
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

    # listen machine events
    thread1 = threading.Thread(name='listen_machine_events', target=dbs.consume, args=('machine_events_exchange', 'machine_events_queue', 'machine_event', process_machine_event), daemon=True)
    thread1.start()
    app.run(host=config['framework']['rsrc_mgr']['host'], port=5000, debug=True)
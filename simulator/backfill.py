from random import randint
from bintrees import FastRBTree
import databus as dbs
import threading
from threading import RLock
import json
import time
from datetime import datetime
import pymongo
import requests
import pandas as pd
from random import randint
from monitor import Monitor
from utils import *


monitor = Monitor()
get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def process_job():
    global waiting_queue, running_job_tree
    while True:
        if len(waiting_queue) == 0:
            continue
        osg_job = waiting_queue[0]
        osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
        flag = False
        # find a node from osg pool
        response = requests.post(url='%s/find' % rsrc_mgr_url, json={"pool": "osg"})
        if response.status_code == 200:
            osg_nodes = pd.DataFrame(response.json())
            osg_nodes = osg_nodes[(osg_nodes['cpus'] - osg_nodes['inuse_cpus'] >= osg_job['CpusProvisioned']) & (osg_nodes['memory'] - osg_nodes['inuse_memory'] >= osg_job['MemoryProvisioned'])]
            if osg_nodes.shape[0] > 0:
                ff_node = osg_nodes.iloc[0]
                # update osg resource pool
                osg_job['Machine'] = ff_node['HOST_NAME (PHYSICAL)'] 
                osg_job['JobSimStatus'] = 'running'
                osg_job['JobSimLastStartDate'] = datetime.now().timestamp()
                osg_job['JobSimExpectCompleteDate'] = osg_job['JobSimLastStartDate'] + osg_job['JobDuration']
                update_req = {
                    "filter": {"HOST_NAME (PHYSICAL)": osg_job['Machine']},
                    "operations": {
                        "$inc": {
                            "inuse_cpus": int(osg_job['CpusProvisioned']), 
                            "inuse_memory": int(osg_job['MemoryProvisioned'])
                        },
                        "$push": {"backfill": osg_job}
                    },
                    "one": True
                }
                response = requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)
                if response.status_code == 200:
                    # moving job from waiting queue to running queue
                    waiting_queue.pop(0)
                    temp = osg_job['JobSimExpectCompleteDate'] + randint(0, 1000000)/1000000
                    running_job_tree[temp] = osg_job
                    flag = True
        
        if not flag:
            # acquire node from chameleon pool
            payload = {"node_type": "compute_haswell", "node_cnt": 1, "pool": "osg"}
            requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)


def measure_inuse_nodes():
    inuse_machines = []
    for key in running_job_tree.keys():
        machine = running_job_tree[key]['Machine']
        inuse_machines.append(machine)
    json_body = [{'measurement': 'osg_inuse', 'fields': {'osg-inuse': len(set(inuse_machines))}}]
    monitor.influx_client.write_points(json_body)


def trace_active_job():
    terminates = 0
    completes = 0
    while True:
        if config['simulation']['enable_monitor']:
            monitor.monitor_osg_jobs(running_job_tree.count, len(waiting_queue), completes, terminates)
            measure_inuse_nodes()
        while not running_job_tree.is_empty():
            end_date, osg_job = running_job_tree.min_item()
            if osg_job['JobDuration'] <= scale_ratio*(datetime.now().timestamp() - osg_job['JobSimLastStartDate']):
                update_req = {
                    "filter": {"$and":[{"HOST_NAME (PHYSICAL)": osg_job['Machine']}, {"pool": "osg"}]},
                    "operations": {
                        "$inc": {
                            "inuse_cpus": -int(osg_job['CpusProvisioned']), 
                            "inuse_memory": -int(osg_job['MemoryProvisioned'])
                        },
                        "$pull": {"backfill": {"GlobalJobId": osg_job['GlobalJobId']}}
                    },
                    "one": True
                }
                response = requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)
                if response.status_code == 200:
                    osg_job['JobSimStatus'] = 'completed'
                    osg_job['JobSimCompleteDate'] = osg_job['JobSimLastStartDate'] + osg_job['JobDuration']
                    osg_job_collection.insert_one(osg_job)
                    running_job_tree.remove(end_date)
                    completes += 1
                else:
                    osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
                    if osg_job['ResubmitCount'] == 0:
                        terminates += 1
                    osg_job['ResubmitCount'] += 1
                    osg_job['JobSimStatus'] = 'pending'
            else:
                break


def receive_job(ch, method, properties, body):
    global waiting_queue
    if properties.headers['key'] == 'osg_job':
        osg_job = json.loads(body)
        osg_job.update({
            "JobSimSubmitDate": datetime.now().timestamp(),
            "JobSimLastSubmitDate": None,
            "JobSimLastStartDate": None,
            "JobSimExpectCompleteDate": None,
            "JobSimCompleteDate": None,
            "JobSimStatus": "pending",
            "ResubmitCount": 0,
            "Machine": None
        })
        osg_job['GlobalJobId'] = osg_job['GlobalJobId'].replace('.', '_')
        waiting_queue.append(osg_job)


def terminate_job(ch, method, properties, body):
    global running_job_tree, waiting_queue
    if properties.headers['key'] == 'terminate_osg_job':
        backfills = json.loads(body)['backfills']
        for i in range(len(backfills)):
            osg_job = backfills[i]
            osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
            osg_job['ResubmitCount'] += 1
            osg_job['JobSimStatus'] = 'pending'
            if osg_job['JobSimExpectCompleteDate'] in running_job_tree.keys():
                running_job_tree.remove(key=osg_job['JobSimExpectCompleteDate'])
                waiting_queue.append(osg_job)


if __name__ == '__main__':
    config = load_config()
    scale_ratio = get_scale_ratio(config)
    rsrc_mgr_url = get_rsrc_mgr_url(config)

    mongo_client = pymongo.MongoClient(get_mongo_url(config))
    db = mongo_client['ChameleonSimulator']
    db.drop_collection('osg_jobs')
    osg_job_collection = db['osg_jobs']

    waiting_queue = []
    running_job_tree = FastRBTree()

    thread1 = threading.Thread(name='listen_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', receive_job), daemon=True)
    thread2 = threading.Thread(name='terminate_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'terminate_osg_job', terminate_job), daemon=True)
    thread3 = threading.Thread(name='trace_active_jobs', target=trace_active_job, daemon=True)
    thread1.start()
    thread2.start()
    thread3.start()
    process_job()
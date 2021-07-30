from logging import Logger
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
from collections import deque
from multiprocessing import Pool


monitor = Monitor()
get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def process_job():
    global waiting_queue, running_jobs_pool, running_job_count
    while True:
        if len(waiting_queue) == 0:
            continue
        osg_job = waiting_queue[0]
        osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
        agg_body = {'steps': [
            {"$match": {"pool": "osg", "ready_for_osg": {"$lte": datetime.now().timestamp()}}},
            {"$project": {
                "HOST_NAME (PHYSICAL)": 1,
                "free_cpus": {"$subtract": ["$cpus", "$inuse_cpus"]}, 
                "free_memory": {"$subtract": ["$memory", "$inuse_memory"]}
            }},
            {"$match": {
                "free_cpus": {"$gte": osg_job['CpusProvisioned']},
                "free_memory": {"$gte": osg_job['MemoryProvisioned']}}
            },
            {"$limit": 1}
        ]}
        response = requests.post(url='%s/aggregate' % rsrc_mgr_url, json=agg_body)
        if response.status_code == 200:
            osg_job['Machine'] = response.json()[0]['HOST_NAME (PHYSICAL)']
            osg_job['JobSimStatus'] = 'running'
            update_req = {
                "filter": {"$and": [{"HOST_NAME (PHYSICAL)": osg_job['Machine']}, {"pool": "osg"}] },
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
                waiting_queue.popleft()
                osg_job['JobSimLastStartDate'] = datetime.now().timestamp()
                running_job_count += 1
                running_jobs_pool.apply_async(wait_job_complete, (osg_job, ), callback=job_execution_result)
                continue
        
        # acquire node from chameleon pool
        payload = {"node_type": "compute_haswell", "node_cnt": 1, "pool": "osg"}
        requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)


def wait_job_complete(osg_job):
    sleep(osg_job['JobDuration']/scale_ratio)
    return osg_job


def job_execution_result(osg_job):
    global completed_job_count, running_job_count, osg_job_collection, waiting_queue
    update_req = {
        "filter": {
            "$and":[
                {"HOST_NAME (PHYSICAL)": osg_job['Machine']}, 
                {"pool": "osg"},
                {"backfill": {"$elemMatch": {"GlobalJobId": osg_job['GlobalJobId']}}}]
        },
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
        osg_job['JobSimCompleteDate'] = datetime.now().timestamp()
        osg_job_collection.replace_one(filter={'GlobalJobId': osg_job['GlobalJobId']}, replacement=osg_job, upsert=True)
        completed_job_count += 1
        running_job_count -= 1
        if config['simulation']['enable_monitor']:
            monitor.monitor_osg_jobs(running_job_count, len(waiting_queue), completed_job_count, terminate_job_count)
    else:
        waiting_queue.appendleft(osg_job)


def receive_job(ch, method, properties, body):
    global waiting_queue
    if properties.headers['key'] == 'osg_job':
        osg_job = json.loads(body)
        osg_job.update({
            "JobSimSubmitDate": datetime.now().timestamp(),
            "JobSimLastSubmitDate": None,
            "JobSimLastStartDate": None,
            "JobSimCompleteDate": None,
            "JobSimStatus": "pending",
            "ResubmitCount": 0,
            "Machine": None
        })
        osg_job['GlobalJobId'] = osg_job['GlobalJobId'].replace('.', '_')
        waiting_queue.append(osg_job)


def terminate_job(ch, method, properties, body):
    global running_jobs_pool, waiting_queue, terminate_job_count, completed_job_count
    if properties.headers['key'] == 'terminate_osg_job':
        message = json.loads(body)
        backfills = message['backfills']
        for i in range(len(backfills)):
            osg_job = backfills[i]
            if osg_job_collection.find({"GlobalJobId": osg_job['GlobalJobId']}).count() == 0:
                print('terminate job: %s' % backfills[i]['GlobalJobId'])
                osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
                osg_job['ResubmitCount'] = osg_job['ResubmitCount'] + 1
                osg_job['JobSimStatus'] = 'pending'
                waiting_queue.appendleft(osg_job)
                terminate_job_count += 1


if __name__ == '__main__':
    config = load_config()
    scale_ratio = get_scale_ratio(config)
    rsrc_mgr_url = get_rsrc_mgr_url(config)

    mongo_client = pymongo.MongoClient(get_mongo_url(config))
    db = mongo_client['ChameleonSimulator']
    db.drop_collection('osg_jobs')
    osg_job_collection = db['osg_jobs']
    osg_job_collection.create_index("GlobalJobId")

    running_jobs_pool = Pool(10)
    running_job_count = 0
    completed_job_count = 0
    terminate_job_count = 0

    waiting_queue = deque([])
    running_job_tree = FastRBTree()

    thread1 = threading.Thread(name='listen_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', receive_job, 'push'), daemon=True)
    thread2 = threading.Thread(name='terminate_osg_jobs', target=dbs.consume, args=('internal_exchange', 'internal_queue', 'terminate_osg_job', terminate_job, 'push'), daemon=True)
    # thread3 = threading.Thread(name='trace_active_jobs', target=trace_active_job, daemon=True)
    thread1.start()
    thread2.start()
    # thread3.start()
    process_job()
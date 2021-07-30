from inspect import trace
from logging import Logger
from os import name
import os
from random import randint
from typing import overload
from bintrees import FastRBTree
from numpy import exp
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
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, thread



monitor = Monitor()
get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def compute_cost(compute_time, osg_job):
    return compute_time * osg_job['CpusProvisioned'] * osg_job['CPUsUsage']


def start_job_cb(osg_job):
    global waiting_queue, jobs_start_time
    osg_job['JobSimStartDate'] = datetime.now().timestamp()
    update_req = {
        "filter": {"$and": [
            {"pool": "osg"},
            {"free_cpus": {"$gte": osg_job['CpusProvisioned']}},
            {"free_memory": {"$gte": osg_job['MemoryProvisioned']}}
        ]},
        "operations": {
            "$set": {"status": "inuse"},
            "$inc": {
                "free_cpus": -int(osg_job['CpusProvisioned']), 
                "free_memory": -int(osg_job['MemoryProvisioned'])
            },
            "$push": {"backfill": osg_job}
        },
        "one": True
    }

    resp = requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)
    if resp.status_code == 200:
        body = resp.json()
        osg_job['Machine'] = body['HOST_NAME (PHYSICAL)']
        osg_job['JobSimStartDate'] = datetime.now().timestamp()
        osg_job['JobExpectEndDate'] = osg_job['JobSimStartDate'] + osg_job['JobDuration']/scale_ratio
        osg_job['JobSimCompleteDate'] = osg_job['JobExpectEndDate']
        running_job_dict[osg_job['GlobalJobId']] = osg_job
        osg_job_collection.replace_one({"GlobalJobId": osg_job['GlobalJobId']}, osg_job, upsert=True)
        return
    
    waiting_queue.appendleft(osg_job)
    # acquire node from chameleon pool
    payload = {"node_type": "compute_haswell", "node_cnt": 1, "pool": "osg"}
    requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)


def start_job(callback):
    global waiting_queue, running_job_dict
    while True:
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = []
            for i in range(100):
                if len(waiting_queue) == 0:
                    continue
                osg_job = waiting_queue.popleft()
                future = executor.submit(start_job_cb, osg_job)
                futures.append(future)
        if completed_job_count > 0 and len(waiting_queue) == 0 and len(running_job_dict) == 0:
            break
    
    update_req = {
        "filter": {"pool": "osg"},
        "operations": {
            "$set": {
                "pool": "chameleon",
                "status": "free",
                "free_cpus": None,
                "free_memory": None,
                "backfill": []
            }
        },
        "one": False
    }
    requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)

        

def stop_job_cb(action_date, osg_job):
    global completed_job_count
    update_req = {
        "filter": {
            "$and":[
                {"HOST_NAME (PHYSICAL)": osg_job['Machine']},
                {"backfill": {"$elemMatch": {"GlobalJobId": osg_job['GlobalJobId']}}}]
        },
        "operations": {
            "$inc": {
                "free_cpus": int(osg_job['CpusProvisioned']), 
                "free_memory": int(osg_job['MemoryProvisioned'])
            },
            "$pull": {"backfill": {"GlobalJobId": osg_job['GlobalJobId']}}
        },
        "one": True
    }
    resp = requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)
    if resp.status_code == 200:
        completed_job_count += 1
        now = datetime.now().timestamp()
        osg_job_collection.update_one(
            filter={"GlobalJobId": osg_job['GlobalJobId']}, 
            update={
                "$set": {"JobSimCompleteDate": now, "action": "stop", "JobDuration": float(now-osg_job['JobSimStartDate'])},
                "$inc": {"TotalCost": compute_cost(now-osg_job['JobSimStartDate'], osg_job)}}
        )
        del running_job_dict[osg_job['GlobalJobId']]
        if config['simulation']['enable_monitor']:
            monitor.monitor_osg_jobs(len(running_job_dict), len(waiting_queue), completed_job_count, terminate_job_count)


def stop_job(callback):
    global completed_job_count, waiting_queue, running_job_dict
    while True:
        now = datetime.now().timestamp()
        if len(running_job_dict) == 0:
            continue
                
        with ThreadPoolExecutor(max_workers=12) as executor:
            for jid in list(running_job_dict.keys()):
                try:
                    action_date = running_job_dict[jid]['JobExpectEndDate']
                    if now >= action_date:
                        # print(now-action_date)
                        future = executor.submit(stop_job_cb, action_date, running_job_dict[jid])
                except KeyError as ex:
                    print('error:', ex)
                


def receive_job(ch, method, properties, body):
    global waiting_queue
    if properties.headers['key'] == 'osg_job':
        osg_job = json.loads(body)
        now = datetime.now().timestamp()
        osg_job.update({
            "GlobalJobId": osg_job['GlobalJobId'].replace('.', '_'),
            "JobSimSubmitDate": now,
            "JobSimStartDate": None,
            "JobExpectEndDate": None,
            "JobSimCompleteDate": None,
            "ResubmitCount": 0,
            "TotalCost": 0,
            "WastedCost": 0,
            "Machine": None
        })
        waiting_queue.append(osg_job)


def terminate_job(ch, method, properties, body):
    global waiting_queue, terminate_job_count, completed_job_count
    if properties.headers['key'] == 'terminate_osg_job':
        body = json.loads(body)
        backfills = body['backfills']
        for i in range(len(backfills)):
            osg_job = backfills[i]
            # print('terminate:', osg_job['GlobalJobId'])
            osg_job['ResubmitCount'] += 1
            osg_job['action'] = 'start'
            
            if osg_job['GlobalJobId'] in running_job_dict:
                sim_start_date = running_job_dict[osg_job['GlobalJobId']]['JobSimStartDate']
                interrupt_date = body['time']
            else:
                temp = osg_job_collection.find_one({"GlobalJobId": osg_job['GlobalJobId']})
                if temp:
                    sim_start_date = temp['JobSimStartDate']
                    interrupt_date = min(temp['JobSimCompleteDate'], body['time'])
                else:
                    continue
            
            osg_job['WastedCost'] += compute_cost(interrupt_date - sim_start_date, osg_job)
            osg_job['TotalCost'] += compute_cost(interrupt_date - sim_start_date, osg_job)
            
            """
            Due to time compression, the job may have been removed from running_job_dict 
            before it receive the termination msg.
            Given the job running time is >> code operation time, we regard jobs always need to be 
            terminated as long as resource manager issued the command.
            """
            waiting_queue.appendleft(osg_job)


if __name__ == '__main__':
    config = load_config()
    scale_ratio = get_scale_ratio(config)
    rsrc_mgr_url = get_rsrc_mgr_url(config)

    mongo_client = pymongo.MongoClient(get_mongo_url(config))
    db = mongo_client['ChameleonSimulator']
    db.drop_collection('osg_jobs')
    osg_job_collection = db['osg_jobs']

    completed_job_count = 0
    terminate_job_count = 0

    lock = RLock()
    queue_lock = RLock()
    tree_lock = RLock()

    waiting_queue = deque([])
    running_job_dict = {}

    thread1 = threading.Thread(name='listen_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', receive_job, 'push'), daemon=True)
    thread2 = threading.Thread(name='terminate_osg_jobs', target=dbs.consume, args=('internal_exchange', 'internal_queue', 'terminate_osg_job', terminate_job, 'push'), daemon=True)
    thread3 = threading.Thread(name='stop_jobs', target=stop_job, args=(stop_job_cb,), daemon=True)
    thread1.start()
    thread2.start()
    thread3.start()
    start_job(start_job_cb)
    thread1.join()
    thread2.join()
    thread3.join()
    
from inspect import trace
from logging import Logger
from os import name
import os
from random import randint
from typing import overload
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
import asyncio
import aiohttp



monitor = Monitor()
get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def compute_cost(compute_time, osg_job):
    return compute_time * osg_job['CpusProvisioned'] * osg_job['CPUsUsage']


def start_job_cb(osg_job):
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
    resp1 = requests.post(url='%s/aggregate' % rsrc_mgr_url, json=agg_body)
    if resp1.status_code == 200:
        body = resp1.json()
        osg_job['Machine'] = body[0]['HOST_NAME (PHYSICAL)']
        osg_job['JobSimStartDate'] = datetime.now().timestamp()
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
        
        resp2 = requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)
        if resp2.status_code == 200:
            waiting_queue.popleft()
            osg_job['JobSimStartDate'] = datetime.now().timestamp()
            running_job_tree[osg_job['JobSimStartDate'] + osg_job['JobDuration']/scale_ratio] = osg_job
            osg_job_collection.replace_one({"GlobalJobId": osg_job['GlobalJobId']}, osg_job, upsert=True)
            return

    # acquire node from chameleon pool
    payload = {"node_type": "compute_haswell", "node_cnt": 1, "pool": "osg"}
    requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
    

def start_job(callback):
    global waiting_queue, running_job_tree
    while True:
        if len(waiting_queue) == 0:
            continue
        osg_job = waiting_queue[0]
        callback(osg_job)
        


def stop_job_cb(action_date, osg_job):
    global completed_job_count
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
    resp = requests.post(url='%s/update' % rsrc_mgr_url, json=update_req)
    if resp.status_code == 200:
        completed_job_count += 1
        osg_job_collection.update_one(
            filter={"GlobalJobId": osg_job['GlobalJobId']}, 
            update={
                "$set": {"JobSimCompleteDate": action_date, "action": "stop"},
                "$inc": {"TotalCost": compute_cost(action_date-osg_job['JobSimStartDate'], osg_job)}}
        )
        
        if config['simulation']['enable_monitor']:
            monitor.monitor_osg_jobs(running_job_tree.count, len(waiting_queue), completed_job_count, terminate_job_count)


def stop_job(callback):
    global completed_job_count, waiting_queue, running_job_tree
    while True:
        now = datetime.now().timestamp()
        if running_job_tree.is_empty():
            continue
        action_date, osg_job = running_job_tree.min_item()
        if now >= action_date:
            print(now-action_date)
            running_job_tree.remove(action_date)
            callback(action_date, osg_job)
            

def receive_job(ch, method, properties, body):
    global waiting_queue
    if properties.headers['key'] == 'osg_job':
        osg_job = json.loads(body)
        now = datetime.now().timestamp()
        osg_job.update({
            "GlobalJobId": osg_job['GlobalJobId'].replace('.', '_'),
            "JobSimSubmitDate": now,
            "JobSimStartDate": None,
            "JobSimCompleteDate": None,
            "ResubmitCount": 0,
            "TotalCost": 0,
            "WastedCost": 0,
            "Machine": None
        })
        waiting_queue.append(osg_job)


def terminate_job(method, properties, body):
    global waiting_queue, terminate_job_count, completed_job_count
    if properties.headers['key'] == 'terminate_osg_job':
        body = json.loads(body)
        backfills = body['backfills']
        for i in range(len(backfills)):
            osg_job = backfills[i]
            osg_job['ResubmitCount'] += 1
            osg_job['action'] = 'start'
            
            if (osg_job['JobSimStartDate'] + osg_job['JobDuration']) in running_job_tree.keys():
                sim_start_date = running_job_tree[osg_job['JobSimStartDate'] + osg_job['JobDuration']]['JobSimStartDate']
                running_job_tree.remove(osg_job['JobSimStartDate'] + osg_job['JobDuration'])
            else:
                sim_start_date = osg_job_collection.find_one({"GlobalJobId": osg_job['GlobalJobId']})['JobSimStartDate']
            osg_job['WastedCost'] += compute_cost(body['time'] - sim_start_date, osg_job)
            print(body['time'], sim_start_date)
            osg_job['TotalCost'] += compute_cost(body['time'] - sim_start_date, osg_job)
            
            """
            Due to time compression, the job may have been removed from running_job_tree 
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
    running_job_tree = FastRBTree()

    thread1 = threading.Thread(name='listen_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', receive_job, 'push'), daemon=True)
    thread2 = threading.Thread(name='terminate_osg_jobs', target=dbs.consume, args=('internal_exchange', 'internal_queue', 'terminate_osg_job', terminate_job, 'pull'), daemon=True)
    thread3 = threading.Thread(name='stop_jobs', target=stop_job, args=(stop_job_cb,), daemon=True)
    thread1.start()
    thread2.start()
    thread3.start()
    start_job(start_job_cb)
    
    
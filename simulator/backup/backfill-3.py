from inspect import trace
from logging import Logger
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


async def start_job(action_date, osg_job):
    global waiting_job_tree
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
    osg_job['JobSimStartDate'] = action_date
    async with aiohttp.ClientSession() as session:
        header = {"Content-Type": "application/json;charset=UTF-8"}
        async with session.post(url='%s/aggregate' % rsrc_mgr_url, json=agg_body, headers=header) as resp1:
            if resp1.status == 200:
                body = await resp1.json()
                osg_job['Machine'] = body[0]['HOST_NAME (PHYSICAL)']
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
                
                async with session.post(url='%s/update' % rsrc_mgr_url, json=update_req, headers=header) as resp2:
                    if resp2.status == 200:
                        waiting_job_tree.remove(action_date)
                        osg_job_collection.replace_one({"GlobalJobId": osg_job['GlobalJobId']}, osg_job, upsert=True)
                        return
    
        # acquire node from chameleon pool
        payload = {"node_type": "compute_haswell", "node_cnt": 1, "pool": "osg"}
        async with session.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload) as resp:
            start = datetime.now().timestamp()
            waiting_job_tree[start] = osg_job
            osg_job = osg_job.copy()
            osg_job['action'] = 'stop'
            end = start + osg_job['JobDuration']/scale_ratio
            waiting_job_tree[end] = osg_job


def record_jobs():
    if config['simulation']['enable_monitor']:
        monitor.monitor_osg_jobs(0, 0, completed_job_count, terminate_job_count)


async def stop_job(action_date, osg_job):
    global completed_job_count, waiting_job_tree
    update_req = {
        "filter": {
            "$and":[
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

    async with aiohttp.ClientSession() as session:
        header = {"Content-Type": "application/json;charset=UTF-8"}
        async with session.post(url='%s/update' % rsrc_mgr_url, json=update_req, headers=header) as resp:
            if resp.status == 200:
                # print('stop -- %d' % rc.status_code)
                record = osg_job_collection.find_one({"GlobalJobId": osg_job['GlobalJobId']})
                osg_job_collection.update_one(
                    filter={"GlobalJobId": osg_job['GlobalJobId']}, 
                    update={
                        "$set": {"JobSimCompleteDate": action_date, "action": "stop"},
                        "$inc": {"TotalCoreHours": (action_date-record['JobSimSubmitDate'])* osg_job['CpusProvisioned']}})
                completed_job_count += 1
                waiting_job_tree.remove(action_date)
                record_jobs()
    

async def process_job():
    global waiting_job_tree
    while True:
        if waiting_job_tree.is_empty():
            continue
        action_date, osg_job = waiting_job_tree.min_item()
        if osg_job['action'] == 'start':
            await start_job(action_date, osg_job)
        else:
            await stop_job(action_date, osg_job)

        if not waiting_job_tree.is_empty():
            next_action_date, _ = waiting_job_tree.min_item()
            sleep_sec = next_action_date - action_date
        else:
            sleep_sec = 0
        
        sleep(sleep_sec)


def receive_job(ch, method, properties, body):
    global waiting_job_tree
    if properties.headers['key'] == 'osg_job':
        osg_job = json.loads(body)
        now = datetime.now().timestamp()
        osg_job.update({
            "GlobalJobId": osg_job['GlobalJobId'].replace('.', '_'),
            "JobSimSubmitDate": now,
            "JobSimStartDate": None,
            "JobSimCompleteDate": None,
            "ResubmitCount": 0,
            "TotalCoreHours": 0,
            "WastedCoreHours": 0,
            "Machine": None
        })
        osg_job['action'] = 'start'
        waiting_job_tree[now] = osg_job

        osg_job = osg_job.copy()
        end = now + osg_job['JobDuration']/scale_ratio
        osg_job['action'] = 'stop'
        waiting_job_tree[end] = osg_job


def terminate_job(method, properties, body):
    global waiting_job_tree, terminate_job_count
    if properties.headers['key'] == 'terminate_osg_job':
        body = json.loads(body)
        backfills = body['backfills']
        for i in range(len(backfills)):
            osg_job = backfills[i]
            osg_job['ResubmitCount'] += 1
            osg_job['WastedCoreHours'] += (body['time'] - osg_job['JobSimStartDate']) * osg_job['CpusProvisioned']
            osg_job['TotalCoreHours'] += (body['time'] - osg_job['JobSimStartDate']) * osg_job['CpusProvisioned']
            osg_job['action'] = 'start'
            now = datetime.now().timestamp()
            waiting_job_tree.insert(key=now, value=osg_job)

            osg_job = osg_job.copy()
            osg_job['action'] = 'stop'
            waiting_job_tree.insert(key=now+osg_job['JobDuration']/scale_ratio, value=osg_job)
            terminate_job_count += 1


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
    # waiting_queue = deque([])
    waiting_job_tree = FastRBTree()
    running_job_tree = FastRBTree()

    thread1 = threading.Thread(name='listen_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', receive_job, 'push'), daemon=True)
    thread2 = threading.Thread(name='terminate_osg_jobs', target=dbs.consume, args=('internal_exchange', 'internal_queue', 'terminate_osg_job', terminate_job, 'pull'), daemon=True)
    thread1.start()
    thread2.start()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_job())
    
from collections import deque
from bintrees import FastRBTree
import databus as dbs
import threading
from threading import RLock
import json
import argparse
import time
from datetime import datetime
import pymongo
import requests
import pandas as pd


get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def submit_job():
    global waiting_queue, running_job_tree
    while True:
        with lock:
            if len(waiting_queue) > 0:
                osg_job = waiting_queue[0]
                osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()

                # wait until some node is available
                while True:
                    # find a node from osg pool
                    osg_nodes = resource_pool.find({"pool": "osg"})
                    osg_nodes = pd.DataFrame(list(osg_nodes))
                    if len(list(osg_nodes)) > 0:
                        osg_nodes = osg_nodes[(osg_nodes['cpus'] - osg_nodes['inuse_cpus'] >= osg_job['CpusProvisioned']) & (osg_nodes['memory'] - osg_nodes['inuse_memory'] >= osg_job['MemoryProvisioned'])]
                        if osg_nodes.shape[0] > 0:
                            break
                    # acquire node from chameleon pool
                    payload = {"node_type": "compute_haswell", "node_cnt": 1, "pool": "osg"}
                    requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
                    
                ff_node = osg_nodes.iloc[0]
                # update osg resource pool
                osg_job['Machine'] = ff_node['HOST_NAME (PHYSICAL)'] 
                osg_job['JobSimStatus'] = 'running'
                osg_job['JobSimLastStartDate'] = datetime.now().timestamp()
                osg_job['JobSimExpectCompleteDate'] = osg_job['JobSimLastStartDate'] + osg_job['JobDuration']
                ff_node['backfill'].append(osg_job)
                resource_pool.update_one(
                    {"HOST_NAME (PHYSICAL)": osg_job['Machine']},
                    {"$set": {
                        "inuse_cpus": int(ff_node['inuse_cpus'] + osg_job['CpusProvisioned']), 
                        "inuse_memory": int(ff_node['inuse_memory'] + osg_job['MemoryProvisioned']),
                        "backfill": ff_node['backfill']
                    }}
                )

                # moving job from waiting queue to running queue
                running_job_tree[osg_job['JobSimExpectCompleteDate']] = osg_job
                waiting_queue.popleft()

            
def process_job(ch, method, properties, body):
    global waiting_queue, running_job_tree
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
        with lock:
            waiting_queue.append(osg_job)
    elif properties.headers['key'] == 'terminate_osg_job':
        nodes = json.loads(body)['terminate_nodes']
        terminate_nodes = list(resource_pool.find({"$in": {"HOST_NAME (PHYSICAL)": nodes}}))
        for tn in terminate_nodes:
            for osg_job in terminate_nodes[tn]:
                osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
                osg_job['ResubmitCount'] += 1
                osg_job['JobSimStatus'] = 'pending'
                with lock:
                    running_job_tree.remove(key=osg_job[osg_job['JobSimExpectCompleteDate']])
                    waiting_queue.append(osg_job)


def trace_active_jobs():
    global running_job_tree
    while True:
        with lock:
            if not running_job_tree.is_empty():
                osg_job = running_job_tree.min_item()[1]
                if osg_job['JobDuration']/scale_ratio <= (datetime.now().timestamp() - osg_job['JobSimLastStartDate']):
                    running_job_tree.pop_min()
                    osg_job['JobSimStatus'] = 'completed'
                    osg_job['JobSimCompleteDate'] = osg_job['JobSimLastStartDate'] + osg_job['JobDuration']
                    osg_job_collection.insert_one(osg_job)
                    node = resource_pool.find_one({"HOST_NAME (PHYSICAL)": osg_job['Machine']})
                    for i in range(len(node['backfill'])):
                        if node['backfill'][i]['GlobalJobId'] == osg_job['GlobalJobId']:
                            del node['backfill'][i]
                            break
                    resource_pool.update_one(
                        {"HOST_NAME (PHYSICAL)": osg_job['Machine']},
                        {"$set": {
                            "inuse_cpus": node['inuse_cpus'] - osg_job['CpusProvisioned'], 
                            "inuse_memory": node['inuse_memory'] - osg_job['MemoryProvisioned'],
                            "backfill": node['backfill']
                        }}
                    )



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rsrc_mgr_url', type=str, default='http://127.0.0.1:5000', help='URL of the REST resource manager')
    parser.add_argument('--mongo', type=str, default='mongodb://chi-sim:chi-sim@127.0.0.1:27017', help='MongoDB connection URL')
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=10000)
    args = parser.parse_args()
    rsrc_mgr_url = args.rsrc_mgr_url
    scale_ratio = args.scale_ratio

    mongo_client = pymongo.MongoClient(args.mongo)
    db = mongo_client['ChameleonSimulator']
    resource_pool = db['resource_pool']
    osg_job_collection = db['osg_jobs']

    lock = RLock()
    waiting_queue = deque()
    running_job_tree = FastRBTree()
    thread1 = threading.Thread(name='listen_osg_jobs', target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', process_job))
    thread2 = threading.Thread(name='submit_osg_jobs', target=submit_job)
    thread3 = threading.Thread(name='trace_active_osg_jobs', target=trace_active_jobs)
    thread1.start()
    thread2.start()
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()
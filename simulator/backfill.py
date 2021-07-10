from collections import deque
from bintrees import FastRBTree
import databus as dbs
import threading
from threading import RLock
import json
import argparse
from datetime import datetime
import pymongo
import requests


get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def submit_job():
    global waiting_queue, running_job_tree
    while True:
        with lock:
            if len(waiting_queue) > 0:
                osg_job = waiting_queue.popleft()
                osg_job['JobSimLastSubmitDate'] = datetime.now().timestamp()
                rc = requests.post(url='%s/submit_osg_job' % rsrc_mgr_url, json=osg_job, timeout=30000)
                osg_job = rc.json()
                osg_job['JobSimLastStartDate'] = datetime.now().timestamp()
                osg_job['JobSimExpectCompleteDate'] = osg_job['JobSimLastStartDate'] + osg_job['JobDuration']
                running_job_tree[osg_job['JobSimExpectCompleteDate']] = osg_job

            
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
        with lock:
            waiting_queue.append(osg_job)
    elif properties.headers['key'] == 'terminate_osg_job':
        osg_job = json.loads(body)
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
    osg_job_collection = db['osg_completed_jobs']

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
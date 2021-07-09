import databus as dbs
import threading
from threading import RLock
import json
import argparse
from influxdb import InfluxDBClient
from bintrees import FastRBTree
from datetime import datetime
import requests


get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def process_osg_jobs(ch, method, properties, body):
    global resource_pool
    if properties.headers['key'] != 'osg_job':
        return
    osg_job = json.loads(body)
    job_id = osg_job['GlobalJobId']
    # TODO:
    # 1. 将job存入数据库
    # 2. 追踪job何时结束
    # 3. 分配给osg的machine上面跑了多少job


if __name__ == '__main__':
    thread2 = threading.Thread(name='listen_osg_events', target=dbs.consume,
                               args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', process_osg_jobs))
    thread2.start()
    thread2.join()
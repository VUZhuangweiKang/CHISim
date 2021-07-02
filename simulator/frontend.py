from collections import deque
from datetime import datetime
import databus as dbs
import threading
from threading import RLock
import json, pickle
import argparse
from enum import Enum
from influxdb import InfluxDBClient
from bintrees import FastRBTree
from datetime import datetime
import requests
import signal
import time
import pandas as pd
from pika.exchange_type import ExchangeType

get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()

def process_usr_requests(ch, method, properties, body):
    if properties.headers['key'] != 'raw_request':
        return
    global prediction_window, rebuild_model_count, sim_start_time, recent_end, active_leases_tree
    usr_request = json.loads(body)
    if not sim_start_time:
        sim_start_time = datetime.now().timestamp()
    
    with al_lock:
        if usr_request['deleted_at']:
            lease_end = min(get_timestamp(usr_request['end_on']), get_timestamp(usr_request['deleted_at']))
        else:
            lease_end = get_timestamp(usr_request['end_on'])
        active_leases_tree[lease_end] = usr_request
        recent_end = active_leases_tree.min_item()[1]
    
    time_diff = (get_timestamp(usr_request['start_on']) - get_timestamp(usr_request['created_at'])) / 60
    if time_diff > 2:
        request_type = 'in_advance'
    else:
        request_type = 'on_demand'
    
    json_body = [{'measurement': request_type, 'fields': usr_request, 'time': usr_request['start_on']}]
    db_client.write_points(json_body)

    if request_type == 'in_advance':
        print('in_advance')
        payload = {'node_type': usr_request['node_type'], 'node_cnt': usr_request['node_cnt'], 'pool': 'chameleon'}
        requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
    elif request_type == 'on_demand':
        # relay user request to forecaster
        dbs.emit_msg(exchange='internal_exchange', routing_key='on_demand_request', payload=json.dumps(usr_request), channel=ch)
        
        # 如果prediction window为空，则阻塞接收raw requests的线程
        while len(prediction_window) == 0:
            print('prediction window is empty')
        with lock:
            if usr_request['node_cnt'] > prediction_window.popleft()['node_cnt']:
                payload = {'node_type': recent_end['node_type'], 'node_cnt': usr_request['node_cnt']-recent_end['node_cnt'], 'pool': 'chameleon'}
                rv = requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
                if rv.status_code == 202:
                    # 预测值和真实值的差要补齐，如果不够，让resource_scheduler决定要抢占哪些节点
                    usr_request['node_cnt'] = usr_request['node_cnt']-recent_end['node_cnt']
                    dbs.emit_msg(exchange='internal_exchange', routing_key='schedule_resource', payload=json.dumps(usr_request), channel=ch)

        # refresh forecaster
        rebuild_model_count += 1
        if rebuild_model_count >= frequency:
            msg = 'update forecaster'
            dbs.emit_msg(exchange='internal_exchange', routing_key='update_forecaster', payload=msg.encode(), channel=ch)
            rebuild_model_count = 0


def fill_prediction_window(ch, method, properties, body):
    if properties.headers['key'] != 'predicted_request':
        return
    global prediction_window
    with lock:
        prediction_window.extend(json.loads(body))


def trace_active_lease():
    global active_leases_tree, recent_end, sim_start_time
    while True:
        with al_lock:
            if recent_end and not active_leases_tree.is_empty():
                start_time = get_timestamp(recent_end['start_on'])
                end_time = get_timestamp(recent_end['end_on'])
                if (end_time-start_time) <= ((datetime.now().timestamp()-sim_start_time)*scale_ratio):
                    if end_time != get_timestamp(active_leases_tree.min_item()[1]['end_on']):
                        raise
                    else:
                        payload = {'node_type': recent_end['node_type'], 'node_cnt': recent_end['node_cnt']}
                        rlt = requests.post(url='%s/release_nodes' % rsrc_mgr_url, json=payload)
                        if rlt.status_code == 200:
                            active_leases_tree.pop_min()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fs', default=1, type=int, help='forward steps')
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=10000)
    parser.add_argument('--frequency', default=3000, type=int,
                        help='the number of new samples when triggering rebuild model')
    parser.add_argument('--rsrc_mgr', type=str, default='http://127.0.0.1:5000', help='IP of the resource manager')
    args = parser.parse_args()
    rsrc_mgr_url = args.rsrc_mgr
    scale_ratio = args.scale_ratio

    prediction_window = deque(maxlen=args.fs)
    lock = RLock()
    frequency = args.frequency
    rebuild_model_count = 0
    active_leases_tree = FastRBTree()
    sim_start_time = None
    recent_end = None
    al_lock = RLock()

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = InfluxDBClient(host=db_info['host'], username=db_info['username'], password=db_info['password'])
    db_client.drop_database('UserRequests')
    db_client.create_database('UserRequests')
    db_client.switch_database('UserRequests')

    # listen Chameleon user requests
    lcur = threading.Thread(name='listen_user_requests', target=dbs.consume, args=('user_requests_exchange', 'user_requests_queue', 'raw_request', process_usr_requests))
    # listen internal messages from forecaster
    limf = threading.Thread(name='listen_forecaster', target=dbs.consume, args=('internal_exchange', 'internal_queue', 'predicted_request', fill_prediction_window))
    # # thread for tracking lease end_on
    active_lease_thr = threading.Thread(name='trace active lease', target=trace_active_lease, args=())

    lcur.start()
    limf.start()
    active_lease_thr.start()
    lcur.join()
    limf.join()
    active_lease_thr.join()
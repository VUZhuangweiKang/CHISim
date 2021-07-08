from collections import deque
import databus as dbs
import threading
from threading import RLock
import json
import time
import argparse
from influxdb import InfluxDBClient
from bintrees import FastRBTree
from datetime import datetime
import requests

get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def process_usr_requests(ch, method, properties, body):
    if properties.headers['key'] != 'raw_request':
        return
    global sim_start_time, active_leases_tree
    usr_request = json.loads(body)
    if not sim_start_time:
        sim_start_time = datetime.now().timestamp()
    
    with lock:
        if usr_request['deleted_at']:
            lease_end = min(get_timestamp(usr_request['end_on']), get_timestamp(usr_request['deleted_at']))
        else:
            lease_end = get_timestamp(usr_request['end_on'])
        active_leases_tree[lease_end] = usr_request
    
    time_diff = (get_timestamp(usr_request['start_on']) - get_timestamp(usr_request['created_at'])) / 60
    if time_diff > 2:
        request_type = 'in_advance'
    else:
        request_type = 'on_demand'
    
    json_body = [{'measurement': request_type, 'fields': usr_request, 'time': usr_request['start_on']}]
    db_client.write_points(json_body)

    if request_type == 'in_advance':
        payload = {'node_type': usr_request['node_type'], 'node_cnt': usr_request['node_cnt'], 'pool': 'chameleon'}
        requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
    elif request_type == 'on_demand':
        # relay user request to forecaster
        dbs.emit_msg(exchange='internal_exchange', routing_key='on_demand_request', payload=json.dumps(usr_request), channel=ch)


def trace_active_lease():
    global active_leases_tree, sim_start_time
    while True:
        with lock:
            if not active_leases_tree.is_empty():
                recent_end = active_leases_tree.min_item()[1]
                start_time = get_timestamp(recent_end['start_on'])
                end_time = get_timestamp(recent_end['end_on'])
                if (end_time-start_time) <= ((datetime.now().timestamp()-sim_start_time)*scale_ratio):
                    payload = {'node_type': recent_end['node_type'], 'node_cnt': recent_end['node_cnt']}
                    rlt = requests.post(url='%s/release_nodes' % rsrc_mgr_url, json=payload)
                    if rlt.status_code == 200:
                        active_leases_tree.pop_min()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=10000)
    parser.add_argument('--rsrc_mgr', type=str, default='http://127.0.0.1:5000', help='IP of the resource manager')
    args = parser.parse_args()
    rsrc_mgr_url = args.rsrc_mgr
    scale_ratio = args.scale_ratio

    active_leases_tree = FastRBTree()
    sim_start_time = None
    lock = RLock()

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = InfluxDBClient(host=db_info['host'], username=db_info['username'], password=db_info['password'])
    db_client.drop_database('UserRequests')
    db_client.create_database('UserRequests')
    db_client.switch_database('UserRequests')

    # listen Chameleon user requests
    thread1 = threading.Thread(name='listen_user_requests', target=dbs.consume, args=('user_requests_exchange', 'user_requests_queue', 'raw_request', process_usr_requests))
    # thread for tracking lease end_on
    thread2 = threading.Thread(name='trace active lease', target=trace_active_lease, args=())

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
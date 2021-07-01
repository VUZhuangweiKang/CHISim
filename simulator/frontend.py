from collections import deque
from datetime import datetime
import databus as dbs
import threading
from threading import Lock
import json, pickle
import argparse
from enum import Enum
from influxdb import InfluxDBClient
from bintrees import FastRBTree
from datetime import datetime
import requests
import signal
import pandas as pd
from pika.exchange_type import ExchangeType

get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()

def process_usr_requests(ch, method, properties, body):
    global prediction_window, rebuild_model_count, sim_time_delta, recent_end
    usr_request = json.loads(body)
    if properties.headers['key'] == 'raw_request':
        if not sim_time_delta:
            sim_time_delta = datetime.now().timestamp() - get_timestamp(usr_request['start_on'])
        with al_lock:
            active_leases_tree.insert(get_timestamp(usr_request['start_on']), usr_request)
            recent_end = active_leases_tree.min_item()[1]
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

            # refresh forecaster
            rebuild_model_count += 1
            if rebuild_model_count >= frequency:
                msg = 'update forecaster'
                dbs.emit_msg(exchange='internal_exchange', routing_key='update_forecaster', payload=msg.encode(), channel=ch)
                rebuild_model_count = 0
    elif properties.headers['key'] == 'predict_request':
        with lock:
            if len(prediction_window) > 0:
                if usr_request['node_cnt'] > prediction_window.popleft()['node_cnt']:
                    payload = {'node_type': recent_end['node_type'], 'node_cnt': usr_request['node_cnt']-recent_end['node_cnt'], 'pool': 'chameleon'}
                    rv = requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
                    if rv.status_code == 202:
                        # 预测值和真实值的差要补齐，如果不够，让resource_scheduler决定要抢占哪些节点
                        usr_request['node_cnt'] = usr_request['node_cnt']-recent_end['node_cnt']
                        dbs.emit_msg(exchange='internal_exchange', routing_key='schedule_resource', payload=json.dumps(usr_request), channel=ch)


def evaluate(ch, method, properties, body):
    global prediction_window
    with lock:
        prediction_window.extend(json.loads(body))


def trace_active_lease():
    while True:
        with al_lock:
            if recent_end:
                end_on_timestamp = get_timestamp(recent_end['end_on'])
                if end_on_timestamp <= datetime.now().timestamp() - sim_time_delta:
                    if end_on_timestamp != get_timestamp(active_leases_tree.min_item()[1]['end_on']):
                        raise
                    else:
                        print(recent_end)
                        payload = {'node_type': recent_end['node_type'], 'node_cnt': recent_end['node_cnt']}
                        rlt = requests.post(url='%s/release_nodes' % rsrc_mgr_url, json=payload)
                        if rlt.status_code == 200:
                            active_leases_tree.pop_min()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fs', default=1, type=int, help='forward steps')
    parser.add_argument('--frequency', default=3000, type=int,
                        help='the number of new samples when triggering rebuild model')
    parser.add_argument('--rsrc_mgr', type=str, default='http://127.0.0.1:5000', help='IP of the resource manager')
    args = parser.parse_args()
    rsrc_mgr_url = args.rsrc_mgr

    prediction_window = deque(maxlen=args.fs)
    lock = Lock()
    frequency = args.frequency
    rebuild_model_count = 0
    active_leases_tree = FastRBTree()
    sim_time_delta = None
    recent_end = None
    al_lock = Lock()

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = InfluxDBClient(host=db_info['host'], username=db_info['username'], password=db_info['password'])
    db_client.drop_database('UserRequests')
    db_client.create_database('UserRequests')
    db_client.switch_database('UserRequests')

    # listen Chameleon user requests
    lcur = threading.Thread(name='listen_user_requests', target=dbs.consume, args=('user_requests_exchange', 'user_requests_queue', 'raw_request', process_usr_requests))
    # listen internal messages from forecaster
    limf = threading.Thread(name='listen_forecaster', target=dbs.consume, args=('internal_exchange', 'internal_queue', 'predicted_request', evaluate))
    # # thread for tracking lease end_on
    active_lease_thr = threading.Thread(name='trace active lease', target=trace_active_lease, args=())

    lcur.start()
    limf.start()
    active_lease_thr.start()
    lcur.join()
    limf.join()
    active_lease_thr.join()
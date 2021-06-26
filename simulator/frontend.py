from collections import deque
from datetime import datetime
import databus as dbs
import threading
from threading import Lock
import json, pickle
import argparse
from enum import Enum
from influxdb import DataFrameClient
from bintrees import FastRBTree
import datetime
import requests


class RequestType(Enum):
    IN_ADVANCE = 'in_advance'
    ON_DEMAND = 'on_demand'


class InternalQueueFilter(Enum):
    PREDICTED_REQUEST = 'predicted_request'


get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()


def dump_usr_request(ch, method, properties, body):
    if properties['headers']['key'] != InternalQueueFilter.PREDICTED_REQUEST:
        return

    global predict_window, rebuild_model_count, sim_time_delta, recent_end
    usr_request = pickle.loads(body)
    if not sim_time_delta:
        sim_time_delta = datetime.now().timestamp() - get_timestamp(usr_request['start_on'])

    with al_lock.acquire():
        active_leases_tree.insert(get_timestamp(usr_request['start_on']), usr_request)
        recent_end = active_leases_tree.min_item()

    create_at_time_obj = get_timestamp(usr_request['create_at'])
    start_on_time_obj = get_timestamp(usr_request['start_on'])
    time_diff = (start_on_time_obj - create_at_time_obj) / 60
    if time_diff > 2:
        request_type = RequestType.IN_ADVANCE
    else:
        request_type = RequestType.ON_DEMAND
    db_client.write_points(request_type, usr_request)

    if request_type == RequestType.IN_ADVANCE:
        dbs.emit_msg(exchange='frontend', routing_key='in_advance_request', payload=pickle.dumps(usr_request),
                     channel=frontend_channel, connection=dbs_connection)
    elif request_type == RequestType.ON_DEMAND:
        with lock.acquire():
            if len(predict_window) > 0:
                if usr_request['node_cnt'] > predict_window.popleft()['node_cnt']:
                    dbs.emit_msg(exchange='frontend', routing_key='update_on_demand_request',
                                 payload=pickle.dumps(usr_request), channel=frontend_channel, connection=dbs_connection)
        payload = {'node_type': recent_end['node_type'], 'node_cnt': recent_end['node_cnt']}
        rlt = requests.post(url='%s/5000/release_nodes', json=payload)
        rebuild_model_count += 1
        if rebuild_model_count >= frequency:
            msg = 'update forecaster'
            dbs.emit_msg(exchange='frontend', routing_key='update_forecaster', payload=msg.encode(),
                         channel=frontend_channel, connection=dbs_connection)
            rebuild_model_count = 0


def evaluate(ch, method, properties, body):
    global predict_window
    with lock.acquire():
        predict_window.extend(pickle.loads(body.decode()))


def trace_active_lease():
    while True:
        with al_lock:
            end_on_timestamp = get_timestamp(recent_end['end_on'])
            if end_on_timestamp <= datetime.now().timestamp() - sim_time_delta:
                if end_on_timestamp != get_timestamp(active_leases_tree.min_item()['end_on']):
                    raise
                else:
                    payload = {'node_type': recent_end['node_type'], 'node_cnt': recent_end['node_cnt']}
                    rlt = requests.post(url='%s/5000/release_nodes', json=payload)
                    if rlt.status_code == 200:
                        active_leases_tree.pop_min()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fs', default=1, type=int, help='forward steps')
    parser.add_argument('--frequency', default=3000, type=int,
                        help='the number of new samples when triggering rebuild model')
    parser.add_argument('--rsrc_mgr', type=str, help='IP of the resource manager')
    args = parser.parse_args()
    rsrc_mgr_url = args.rsrc_mgr

    predict_window = deque(maxlen=args.fs)
    lock = Lock()
    frequency = args.frequency
    rebuild_model_count = 0
    active_leases_tree = FastRBTree()
    sim_time_delta = None
    recent_end = None
    al_lock = Lock()

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = DataFrameClient(*db_info)

    dbs_connection = dbs.init_connection()
    # producers
    frontend_channel = dbs_connection.channel(channel_number=1)
    dbs.queue_bind(frontend_channel, exchange='frontend', queues=['internal'])

    # listen Chameleon user requests
    lcur = threading.Thread(name='listen_user_requests', target=dbs.consume,
                            args=('frontend', 'user_requests', 'raw_request', dump_usr_request, dbs_connection))
    # listen internal messages from forecaster
    limf = threading.Thread(name='listen_forecaster', target=dbs.consume,
                            args=('frontend', 'internal', 'predicted_request', evaluate, dbs_connection))
    # thread for tracking lease end_on
    active_lease_thr = threading.Thread(name='trace active lease', target=trace_active_lease, args=())

    lcur.start()
    limf.start()
    active_lease_thr.start()
    lcur.join()
    limf.join()
    active_lease_thr.join()
    dbs_connection.close()

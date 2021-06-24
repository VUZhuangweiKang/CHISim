from collections import deque
from datetime import datetime
import databus as dbs
import threading
from threading import Lock
import json, pickle
import argparse
from enum import Enum
from influxdb import DataFrameClient


class RequestType(Enum):
    IN_ADVANCE = 'in_advance'
    ON_DEMAND = 'on_demand'


def dump_usr_request(ch, method, properties, body):
    global predict_window, rebuild_model_count
    usr_request = pickle.loads(body)
    create_at_time_obj = datetime.strptime(usr_request['create_at'], '%Y-%m-%d %H:%M:%S')
    start_on_time_obj = datetime.strptime(usr_request['start_on'], '%Y-%m-%d %H:%M:%S')
    time_diff = (start_on_time_obj - create_at_time_obj).seconds / 60
    if time_diff > 2:
        request_type = RequestType.IN_ADVANCE
    else:
        request_type = RequestType.ON_DEMAND
    db_client.write_points(request_type, usr_request)

    if request_type == RequestType.IN_ADVANCE:
        dbs.emit_msg(exchange='frontend', routing_key='in_advance_request', payload=pickle.dumps(usr_request), channel=frontend_channel, connection=dbs_connection)
    elif request_type == RequestType.ON_DEMAND:
        with lock.acquire():
            if len(predict_window) > 0:
                if usr_request['node_cnt'] > predict_window.popleft()['node_cnt']:
                    dbs.emit_msg(exchange='frontend', routing_key='update_on_demand_request',
                                 payload=pickle.dumps(usr_request), channel=frontend_channel, connection=dbs_connection)
        dbs.emit_msg(exchange='frontend', routing_key='on_demand_request', payload=pickle.dumps(usr_request),
                     channel=frontend_channel, connection=dbs_connection)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fs', default=1, type=int, help='forward steps')
    parser.add_argument('--frequency', default=3000, type=int,
                        help='the number of new samples when triggering rebuild model')
    args = parser.parse_args()
    predict_window = deque(maxlen=args.fs)
    lock = Lock()
    frequency = args.frequency
    rebuild_model_count = 0

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = DataFrameClient(*db_info)

    dbs_connection = dbs.init_connection()
    # producers
    frontend_channel = dbs_connection.channel(channel_number=1)
    dbs.queue_bind(frontend_channel, exchange='frontend', queues=['internal'])

    # listen Chameleon user requests
    lcur = threading.Thread(name='listen_user_requests', target=dbs.consume, args=('frontend', 'user_requests', 'raw_request', dump_usr_request, dbs_connection))
    # listen internal messages from forecaster
    limf = threading.Thread(name='listen_forecaster', target=dbs.consume, args=('frontend', 'internal', 'predicted_request', evaluate, dbs_connection))

    lcur.start()
    limf.start()
    lcur.join()
    limf.join()
    dbs_connection.close()




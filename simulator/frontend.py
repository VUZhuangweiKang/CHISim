from collections import deque
from datetime import datetime
import databus as dbs
import threading
from threading import Lock
import json
import argparse
from enum import Enum
from influxdb import InfluxDBClient

dbs_conn = None
db_client = None
rsc_sch_channel = None

req_frcst_channel = None
lock = Lock()
predict_window = deque(maxlen=1000)


class RequestType(Enum):
    IN_ADVANCE = 1
    ON_DEMAND = 2


def dump_usr_request(ch, method, properties, body):
    usr_request = json.loads(body.decode())
    create_at_time_obj = datetime.strptime(usr_request['create_at'], '%Y-%m-%d %H:%M:%S')
    start_on_time_obj = datetime.strptime(usr_request['start_on'], '%Y-%m-%d %H:%M:%S')
    time_diff = (start_on_time_obj - create_at_time_obj).seconds / 60
    if time_diff > 2:
        request_type = RequestType.IN_ADVANCE
    else:
        request_type = RequestType.ON_DEMAND

    json_body = [
        {
            "measurement": request_type,
            "fields": usr_request
        }
    ]
    db_client.write_points(json_body)

    if request_type == RequestType.IN_ADVANCE:
        dbs.emit_msg(exchange='chameleon_user', routing_key='in_advance_request', payload=json.dumps(usr_request), channel=rsc_sch_channel, connection=dbs_conn)
    elif request_type == RequestType.ON_DEMAND:
        with lock.acquire():
            if len(predict_window) > 0:
                left = predict_window.popleft()
                if usr_request['node_cnt'] > left['node_cnt']:
                    dbs.emit_msg(exchange='chameleon_user', routing_key='update_on_demand_request', payload=json.dumps(usr_request), channel=rsc_sch_channel, connection=dbs_conn)
        dbs.emit_msg(exchange='chameleon_user', routing_key='on_demand_request', payload=json.dumps(usr_request), channel=req_frcst_channel, connection=dbs_conn)


def evaluate(ch, method, properties, body):
    global predict_window
    with lock.acquire():
        predict_window.extend(json.loads(body.decode()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sw', default=1000, type=int, help='slide window size')
    args = parser.parse_args()

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = InfluxDBClient(*db_info)

    dbs_conn = dbs.init_connection()
    rsc_sch_channel = dbs_conn.channel(channel_number=1)
    req_frcst_channel = dbs_conn.channel(channel_number=2)

    user_event_handler = threading.Thread(name='user_request_handler',
                                          target=dbs.consume,
                                          args=('chameleon_user', 'user_requests', 'raw_request', dump_usr_request))
    feedback_handler = threading.Thread(name='user_request_handler',
                                          target=dbs.consume,
                                          args=('chameleon_user', 'forecaster_feedback', 'predicted_request', evaluate))
    user_event_handler.start()
    feedback_handler.start()
    user_event_handler.join()
    feedback_handler.join()
    dbs_conn.close()




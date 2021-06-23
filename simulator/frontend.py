from collections import deque
from datetime import datetime
import databus as dbs
import threading
from threading import Lock
import json
import argparse
from enum import Enum
from influxdb import InfluxDBClient

dbs_connection = None
db_client = None
frontend_channel = None
lock = Lock()
predict_window = None


class RequestType(Enum):
    IN_ADVANCE = 1
    ON_DEMAND = 2


def dump_usr_request(ch, method, properties, body):
    global predict_window
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
        dbs.emit_msg(exchange='frontend', routing_key='in_advance_request', payload=json.dumps(usr_request), channel=frontend_channel, connection=dbs_connection)
    elif request_type == RequestType.ON_DEMAND:
        with lock.acquire():
            if len(predict_window) > 0:
                if usr_request['node_cnt'] > predict_window.popleft()['node_cnt']:
                    dbs.emit_msg(exchange='frontend', routing_key='update_on_demand_request',
                                 payload=json.dumps(usr_request), channel=frontend_channel, connection=dbs_connection)
        dbs.emit_msg(exchange='frontend', routing_key='on_demand_request', payload=json.dumps(usr_request),
                     channel=frontend_channel, connection=dbs_connection)


def evaluate(ch, method, properties, body):
    global predict_window
    with lock.acquire():
        predict_window.extend(json.loads(body.decode()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fs', default=1000, type=int, help='forward steps')
    args = parser.parse_args()
    predict_window = deque(maxlen=args.fs)

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = InfluxDBClient(*db_info)

    dbs_connection = dbs.init_connection()
    # producers
    frontend_channel = dbs_connection.channel(channel_number=1)
    dbs.queue_bind(frontend_channel, exchange='frontend', queues=['internal'])

    # consumers
    user_event_handler = threading.Thread(name='user_request_handler',
                                          target=dbs.consume,
                                          args=('frontend', 'user_requests', 'raw_request', dump_usr_request, dbs_connection))
    feedback_handler = threading.Thread(name='feedback_handler',
                                          target=dbs.consume,
                                          args=('frontend', 'internal', 'predicted_request', evaluate, dbs_connection))
    user_event_handler.start()
    feedback_handler.start()
    user_event_handler.join()
    feedback_handler.join()
    dbs_connection.close()




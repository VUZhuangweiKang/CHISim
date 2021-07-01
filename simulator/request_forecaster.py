import databus as dbs
import pickle
from collections import deque
from datetime import datetime
import argparse
import threading
from tensorflow import keras
from utils import *
from enum import Enum


def predict_requests(ch, method, properties, body):
    if properties.headers['key'] != 'on_demand_request':
        return
    global sw_start_time
    request = pickle.loads(body)
    if len(slide_window) == 0:
        sw_start_time = datetime.strptime(request['start_on'], '%Y-%m-%d %H:%M:%S')
    current_time = datetime.strptime(request['start_on'], '%Y-%m-%d %H:%M:%S')
    if (current_time - sw_start_time).seconds / 3600 >= look_back_len:
        df = pd.DataFrame(slide_window)
        df, scaler = data_preprocess(df, resample_slot)
        pred_requests = forecaster.predict(df)
        pred_requests = scaler.inverse_transform(pred_requests)
        dbs.emit_msg(exchange='forecaster', routing_key='schedule_resource', payload=pickle.dumps(pred_requests), channel=forecaster_channel)
        dbs.emit_msg(exchange='forecaster', routing_key='forecaster_feedback', payload=pickle.dumps(pred_requests), channel=forecaster_channel)
    else:
        slide_window.append(request)


def update_forecaster(ch, method, properties, body):
    if properties.headers['key'] != 'update_forecaster':
        return
    global forecaster
    if body.decode() == 'do update forecaster':
        forecaster = keras.models.load_model('forecaster')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--look_back_len', type=int, default=168, help='look back window size in hours, default 1 week')
    parser.add_argument('--slot', type=int, default=3, help='resample time in hours')
    args = parser.parse_args()

    slide_window = deque(maxlen=100000)
    look_back_len = args.look_back_len
    resample_slot = args.slot
    sw_start_time = None
    forecaster = keras.models.load_model('forecaster')

    dbs_connection = dbs.init_connection()
    # producers
    forecaster_channel = dbs_connection.channel()
    dbs.queue_bind(forecaster_channel, exchange='forecaster', queues=['internal'])

    # consumers
    listen_forecaster_update = threading.Thread(name='listen_forecaster_update', target=dbs.consume, args=('forecaster', 'internal', 'do_update_forecaster', update_forecaster))
    listen_frontend = threading.Thread(name='listen_frontend', target=dbs.consume, args=('forecaster', 'internal', 'on_demand_request', predict_requests))
    listen_forecaster_update.start()
    listen_frontend.start()
    listen_forecaster_update.join()
    listen_frontend.join()
    dbs_connection.close()

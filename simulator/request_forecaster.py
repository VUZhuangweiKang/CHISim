import databus as dbs
import pickle
import json
import pandas as pd
from collections import deque
from datetime import datetime
import argparse
import threading
from tsmoothie.smoother import SpectralSmoother
from sklearn.preprocessing import MinMaxScaler

slide_window = None
look_back_len = None
resample_slot = None

start_time = end_time = None
forecaster = None
forecaster_channel = None


def spectral_smoother(df):
    sds = df.values
    smoother = SpectralSmoother(smooth_fraction=0.3, pad_len=look_back_len)
    smoother.smooth(sds)
    return smoother


def resample_sum(data, slot):
    rs_sum = pd.DataFrame([])
    rs_sum['start_on'] = pd.to_datetime(data.start_on)
    rs_sum['node_cnt'] = data.node_cnt
    rs_sum = rs_sum.set_index('start_on', drop=True)
    rs_sum = rs_sum.resample('%dH' % slot).sum().fillna(0)
    rs_sum.reset_index(inplace=True)
    return rs_sum


def predict_requests(ch, method, properties, body):
    global start_time
    request = json.loads(body.decode())
    if len(slide_window) == 0:
        start_time = datetime.strptime(request['start_on'], '%Y-%m-%d %H:%M:%S')
    current_time = datetime.strptime(request['start_on'], '%Y-%m-%d %H:%M:%S')
    if (current_time - start_time).seconds / 3600 >= look_back_len:
        df = pd.DataFrame(slide_window)

        # step 1. re-sample
        df = resample_sum(df, resample_slot)
        df.dropna(inplace=True)
        df = df.astype(float)

        # step 2. smooth data
        smoother = spectral_smoother(df)
        smooth_df = smoother.smooth_data.squeeze()
        df['node_cnt'] = smooth_df

        # step 3. data normalization
        # TODO: validate the efficiency of MinMaxScaler
        scaler = MinMaxScaler(feature_range=(0, 1))
        df = scaler.fit_transform(df)

        # step 4. predict the node_cnt of the next request
        pred_requests = forecaster.predict(df)
        pred_requests = scaler.inverse_transform(pred_requests)
        dbs.emit_msg(exchange='forecaster', routing_key='forecaster_feedback',
                     payload=pickle.dumps(pred_requests), channel=forecaster_channel)
    else:
        slide_window.append(request)


def update_forecaster(ch, method, properties, body):
    global forecaster
    assert body.decode() == 'update forecaster'
    with open('forecaster.pkl', 'rb') as f:
        forecaster = pickle.load(f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--look_back_len', type=int, default=168, help='look back window size in hours, default 1 week')
    parser.add_argument('--slot', type=int, default=3, help='resample time in hours')
    args = parser.parse_args()
    slide_window = deque(maxlen=100000)
    look_back_len = args.look_back_len
    resample_slot = args.slot

    with open('forecaster.pkl', 'rb') as f:
        forecaster = pickle.load(f)

    dbs_connection = dbs.init_connection()
    # producers
    forecaster_channel = dbs_connection.channel()
    dbs.queue_bind(forecaster_channel, exchange='forecaster', queues=['internal'])

    # consumers
    forecaster_thr = threading.Thread(name='forecaster_handler', target=dbs.consume,
                                      args=('forecaster', 'internal', 'update_forecaster', update_forecaster, dbs_connection))
    slide_window_thr = threading.Thread(name='slide_window_handler', target=dbs.consume,
                                      args=('forecaster', 'internal', 'on_demand_request', predict_requests, dbs_connection))


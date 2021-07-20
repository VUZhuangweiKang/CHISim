from pymongo import database, monitor
from pymongo.message import query
import databus as dbs
import threading
from threading import RLock
import json
import argparse
from influxdb import InfluxDBClient
from bintrees import FastRBTree
from datetime import datetime
import requests
import pymongo
from utils import *
import requests
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import tensorflow as tf
import math
import time
import numpy as np
import pandas as pd
from random import randint
from monitor import Monitor
import warnings
warnings.filterwarnings("ignore")


logger = get_logger(__name__)
get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()
warm_down = False
monitor = Monitor()


def process_usr_requests(ch, method, properties, body):
    if properties.headers['key'] != 'raw_request':
        return
    global sim_start_time, active_leases_tree, warm_down
    usr_request = json.loads(body)
    usr_request['sim_start_date'] = datetime.now().timestamp()
    time_diff = (get_timestamp(usr_request['start_on']) - get_timestamp(usr_request['created_at'])) / 60
    if time_diff < 0:
        return
    rc = 200
    if time_diff > 2:
        json_body = [{'measurement': 'in_advance', 'fields': usr_request, 'time': usr_request['created_at']}]
        payload = {'node_type': usr_request['node_type'], 'node_cnt': usr_request['node_cnt'], 'pool': 'chameleon'}
        rc = requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload).status_code
    else:
        json_body = [{'measurement': 'on_demand', 'fields': usr_request, 'time': usr_request['start_on']}]
        
        slide_window.append(usr_request)
        if len(prediction_window) > 0:
            if not warm_down:
                logger.info('Frontend has been warmed up')
            warm_down = True
            remaining = prediction_window[0]['node_cnt'] - usr_request['node_cnt']
            if remaining < 0: # since we have reserved node when performing prediction
                payload = {'node_type': usr_request['node_type'], 'node_cnt': usr_request['node_cnt'], 'pool': 'chameleon'}
                rc = requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload).status_code
            prediction_window[0]['node_cnt'] = remaining
        else:
            payload = {'node_type': usr_request['node_type'], 'node_cnt': usr_request['node_cnt'], 'pool': 'chameleon'}
            rc = requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload).status_code
        if config['simulation']['enable_ml']:
            make_prediction()
    influx_client.write_points(json_body)

    if rc == 200:
        if usr_request['deleted_at']:
            lease_end = min(get_timestamp(usr_request['end_on']), get_timestamp(usr_request['deleted_at']))
        else:
            lease_end = get_timestamp(usr_request['end_on'])
        lease_end += randint(0, 100*scale_ratio)/(100*scale_ratio)
        with tree_lock:
            active_leases_tree[lease_end] = usr_request
    else:
        logger.info('failed to deploy: %s' % usr_request['lease_id'])


def trace_active_lease():
    global active_leases_tree, sim_start_time
    completed = 0
    while True:
        if config['simulation']['enable_monitor']:
            monitor.monitor_chameleon(completed)
            # measure_inuse_nodes()
        with tree_lock:
            while not active_leases_tree.is_empty():
                end_time, recent_end = active_leases_tree.min_item()
                start_time = get_timestamp(recent_end['start_on'])
                if (end_time-start_time) <= ((datetime.now().timestamp()-recent_end['sim_start_date'])*scale_ratio):
                    payload = {'node_type': recent_end['node_type'], 'node_cnt': recent_end['node_cnt']}
                    requests.post(url='%s/release_nodes' % rsrc_mgr_url, json=payload)
                    active_leases_tree.remove(end_time)
                    recent_end['sim_end_date'] = datetime.now().timestamp()
                    recent_end['sim_duration'] = recent_end['sim_end_date'] - recent_end['sim_start_date']
                    ch_lease_collection.insert_one(recent_end)
                    completed += 1
                else:
                    break

########## Request Forecaster ###########

def make_prediction():
    global slide_window
    df = pd.DataFrame(slide_window)
    rsw = resample_sum(df, fs_len).iloc[:-1]
    rsw.dropna(inplace=True)
    rsw.set_index(['start_on'], inplace=True)
    rsw = rsw.astype(float)
    # when the sampled slide window is full, make prediction
    if rsw.shape[0] >= int(sw_len/fs_len):
        # some nodes are left in the previous prediction window
        if len(prediction_window) > 0 and prediction_window[0]['node_cnt'] > 0:
            payload = {'node_type': prediction_window[0]['node_type'], 'node_cnt': prediction_window[0]['node_cnt'], 'pool': 'chameleon'}
            requests.post(url='%s/release_nodes' % rsrc_mgr_url, json=payload)
        
        if len(prediction_window) > 0:
            logger.info('pred_error: %d' % prediction_window[0]['node_cnt'])
            prediction_window.pop(0)

        # data smoothing and normalization
        smoother = spectral_smoother(rsw)
        smooth_rsw = smoother.smooth_data.squeeze()
        df = pd.DataFrame([])
        df['node_cnt'] = smooth_rsw
        scaler = MinMaxScaler(feature_range=(0, 1))
        df = scaler.fit_transform(df)
        df = np.array([df[:, 0]])
        df = np.reshape(df, (df.shape[0], 1, df.shape[1]))
        pred_requests = forecaster.predict(df)
        pred_requests = np.ceil(scaler.inverse_transform(pred_requests))
        for preq in pred_requests:
            payload = {'node_type': "compute_haswell", 'node_cnt': int(preq), 'pool': 'chameleon'}
            response = requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
            if response.status_code != 200:
                # predicted node_cnt can't be deployed, give up prediction at this step, add 0 as placeholder
                payload = {'node_type': "compute_haswell", 'node_cnt': 0, 'pool': 'chameleon'}
                requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
            prediction_window.append(payload)

        # move sliding window --> fs_len
        while get_timestamp(slide_window[0]['start_on']) < rsw.index[fs_len].timestamp():
            slide_window.pop(0)


def create_lstm_dataset(ds, lb=1):
    """
    :param ds: a numpy array that you want to convert into a dataset
    :param lb: the number of previous time steps to use as input variables to predict the next time period
    :return: x, y for training LSTM
    """
    data_x, data_y = [], []
    for i in range(len(ds) - lb - 1):
        a = ds[i:(i + lb), 0]
        data_x.append(a)
        data_y.append(ds[i + lb, 0])
    return np.array(data_x), np.array(data_y)


def loss_function(y_true, y_pred):
    alph = 0.5
    loss = tf.exp(tf.cast(tf.where(y_pred<y_true, 1/alph, -1/alph), tf.float32)) * tf.abs(y_pred - y_true)
    return loss


def train_model():
    query_str = 'SELECT * FROM on_demand'
    df = pd.DataFrame(influx_client.query(query_str).get_points())

    """
    Build LSTM Model
    """
    df, scaler = data_preprocess(df, fs_len)
    train, test = train_test_split(df, test_size=0.2, shuffle=False)

    trainX, trainY = create_lstm_dataset(train, sw_len)
    testX, testY = create_lstm_dataset(test, sw_len)

    # reshape input to be [samples, time steps, features]
    trainX = np.reshape(trainX, (trainX.shape[0], 1, trainX.shape[1]))
    testX = np.reshape(testX, (testX.shape[0], 1, testX.shape[1]))

    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(4, input_shape=(1, sw_len), activation='tanh'))
    model.add(Dense(1))

    model.compile(loss=loss_function, optimizer='adam')
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=5)
    model.fit(trainX, trainY, epochs=30, batch_size=1, verbose=1, validation_split=0.2, callbacks=[es])
    model.save('forecaster.h5')

    """
    Evaluate Model
    """
    # make predictions
    trainPredict = model.predict(trainX)
    testPredict = model.predict(testX)

    # invert predictions
    trainPredict = scaler.inverse_transform(trainPredict)
    trainY = scaler.inverse_transform([trainY])
    testPredict = scaler.inverse_transform(testPredict)
    testY = scaler.inverse_transform([testY])

    # calculate root mean squared error
    trainScore = math.sqrt(mean_squared_error(trainY[0], trainPredict[:, 0]))
    testScore = math.sqrt(mean_squared_error(testY[0], testPredict[:, 0]))
    log_txt = {
        'timestamp': datetime.now(),
        'Train Score(RMSE)':  '%.2f' % trainScore,
        'Test Score(RMSE)': '%.2f' % testScore
    }
    logger.info(json.dumps(log_txt))


if __name__ == '__main__':
    config = load_config()
    frc_params = config['framework']['frontend']['request_forecaster']
    scale_ratio = get_scale_ratio(config)
    rsrc_mgr_url =get_rsrc_mgr_url(config)
    
    tree_lock = RLock()
    active_leases_tree = FastRBTree()

    fs_len = frc_params['steps']
    sw_len = frc_params['window']
    slide_window = []
    prediction_window = []

    forecaster = keras.models.load_model('forecaster.h5', compile=False)
    influx_client = InfluxDBClient(**get_influxdb_info(config), database='ChameleonSimulator')

    mongo_client = pymongo.MongoClient(get_mongo_url(config))
    mongodb = mongo_client['ChameleonSimulator']
    ch_lease_collection = mongodb['chameleon_leases']

    thread1 = threading.Thread(name='listen_user_requests', target=dbs.consume, args=('user_requests_exchange', 'user_requests_queue', 'raw_request', process_usr_requests), daemon=True)
    thread2 = threading.Thread(name='trace active lease', target=trace_active_lease, args=(), daemon=True)
    thread1.start()
    thread2.start()
    
    while True:
        if frc_params['retrain']['enabled'] and config['simulation']['enable_ml']:
            query_str = 'SELECT COUNT(lease_id) AS count FROM on_demand'
            query_results = influx_client.query(query_str)
            if 'on_demand' in query_results:
                count = query_results['on_demand']['count'][0]
                if count % frc_params['retrain']['length'] == 0:
                    train_model()
                    forecaster = keras.models.load_model('forecaster.h5', compile=False)
        time.sleep(3)

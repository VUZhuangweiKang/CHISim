import databus as dbs
from collections import deque
import requests
import threading
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import tensorflow as tf
import math
from influxdb import DataFrameClient
import argparse
import time
import numpy as np
import json
from utils import *


# receive live request stream
def listen_on_demand_requests(ch, method, properties, body):
    global forecaster, slide_window
    if properties.headers['key'] != 'on_demand_request':
        return
    request = json.loads(body)
    slide_window.append(request)

    df = pd.DataFrame(slide_window)
    rsw = resample_sum(df, fs_len).iloc[:-1]
    rsw.dropna(inplace=True)
    rsw.set_index(['start_on'], inplace=True)
    rsw = rsw.astype(float)

    if rsw.shape[0] >= int(sw_len/fs_len):
        # 处理预测值和真实值的差距问题，将rsw的最后一个值和prediction window的第一个值比较
        if len(prediction_window) > 0:
            pre_left = prediction_window.popleft()
            rsw_right = json.loads(rsw.iloc[-1].to_json())
            pred_error = rsw_right['node_cnt'] - pre_left['node_cnt']
            if rsw.shape[0] > 1 and pred_error > 0:
                payload = {'node_type': pre_left['node_type'], 'node_cnt': pred_error, 'pool': 'chameleon'}
                # 让resource_manager决定抢占哪些节点如果节点数量不够
                requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)
            elif pred_error < 0:
                payload = {'node_type': pre_left['node_type'], 'node_cnt': -pred_error, 'pool': 'chameleon'}
                requests.post(url='%s/release_nodes' % rsrc_mgr_url, json=payload)

        # 数据平滑处理和正则化
        smoother = spectral_smoother(rsw)
        smooth_rsw = smoother.smooth_data.squeeze()
        df = pd.DataFrame([])
        df['node_cnt'] = smooth_rsw
        scaler = MinMaxScaler(feature_range=(0, 1))
        df = scaler.fit_transform(df)
        df = np.array([df[:, 0]])
        df = np.reshape(df, (df.shape[0], 1, df.shape[1]))
        with lock:
            pred_requests = forecaster.predict(df)
        pred_requests = np.ceil(scaler.inverse_transform(pred_requests))
        for preq in pred_requests:
            payload = {'node_type': "compute_haswell", 'node_cnt': int(preq), 'pool': 'chameleon'}
            prediction_window.append(payload)
            requests.post(url='%s/acquire_nodes' % rsrc_mgr_url, json=payload)

        # move sliding window --> fs_len
        while get_timestamp(slide_window[0]['start_on']) < rsw.index[fs_len].timestamp():
            slide_window.popleft()


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
    df = db_client.query(query_str)

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
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale_ratio', type=float, help='The ratio for scaling down the time series data', default=10000)
    parser.add_argument('--fs_len', default=3, type=int, help='forward prediction window in hours, default 3')
    parser.add_argument('--sw_len', type=int, default=168, help='look back window size in hours, default 1 week')
    parser.add_argument('--rsrc_mgr', type=str, default='http://127.0.0.1:5000', help='IP of the resource manager')
    parser.add_argument('--refresh', default=3000, type=int, help='the number of new samples when triggering rebuild model')
    args = parser.parse_args()
    fs_len = args.fs_len
    sw_len = args.sw_len
    scale_ratio = args.scale_ratio
    slide_window = deque([], maxlen=100000)
    prediction_window = deque([], maxlen=args.fs_len)
    rsrc_mgr_url = args.rsrc_mgr
    lock = threading.RLock()
    forecaster = keras.models.load_model('forecaster.h5', compile=False)
    thread1 = threading.Thread(target=dbs.consume, args=('internal_exchange', 'internal_queue', 'on_demand_request', listen_on_demand_requests))
    thread1.start()

    logger = get_logger(logger_name='build_model', log_file='build_model.log')
    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_info.update({'database': 'UserRequests'})
    db_client = DataFrameClient(**db_info)
    while True:
        query_str = 'SELECT COUNT(lease_id) AS count FROM on_demand'
        query_results = db_client.query(query_str)
        if 'on_demand' in query_results:
            count = query_results['on_demand']['count'][0]
            if count % args.refresh == 0:
                train_model()
                with lock:
                    forecaster = keras.models.load_model('forecaster.h5', compile=False)
        time.sleep(3)

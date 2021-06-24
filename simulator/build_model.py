from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.callbacks import EarlyStopping
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import tensorflow as tf
import math
from influxdb import DataFrameClient
import databus as dbs
import argparse
import threading
import numpy as np
from datetime import datetime
from utils import *
import json


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


def train_model(ch, method, properties, body):
    if body.decode() != 'update forecaster':
        return
    query_str = 'select * from on_demand'
    df = db_client.query(query_str).get_points()

    """
    Build LSTM Model
    """
    df, scaler = data_preprocess(df, resample_slot)
    train, test = train_test_split(df, test_size=test_size, shuffle=False)

    trainX, trainY = create_lstm_dataset(train, look_back_len)
    testX, testY = create_lstm_dataset(test, look_back_len)

    # reshape input to be [samples, time steps, features]
    trainX = np.reshape(trainX, (trainX.shape[0], 1, trainX.shape[1]))
    testX = np.reshape(testX, (testX.shape[0], 1, testX.shape[1]))

    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(4, input_shape=(1, look_back_len), activation='tanh'))
    model.add(Dense(fs))

    model.compile(loss=loss_function, optimizer='adam')
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=5)
    model.fit(trainX, trainY, epochs=30, batch_size=1, verbose=1, validation_split=0.2, callbacks=[es])
    model.save('forecaster')

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

    """
    Notify forecaster to update model
    """
    msg = 'do update forecaster'.encode()
    dbs.emit_msg('build_model', 'do_update_forecaster', msg, dbs_connection, build_model_channel)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--look_back_len', type=int, default=168, help='look back window size in hours, default 1 week')
    parser.add_argument('--fs', default=1, type=int, help='forward steps')
    parser.add_argument('--slot', type=int, default=3, help='resample time in hours')
    parser.add_argument('--test_size', type=float, default=0.2, help='portion of testing dataset')
    args = parser.parse_args()
    look_back_len = args.look_back_len
    fs = args.fs
    resample_slot = args.slot
    test_size = args.test_size
    logger = get_logger(logger_name='build_model', log_file='build_model.log')

    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = DataFrameClient(*db_info)
    dbs_connection = dbs.init_connection()
    build_model_channel = dbs_connection.channel()

    listen_frontend = threading.Thread(name='listen_frontend', target=dbs.consume,
                                       args=('build_model', 'user_requests', 'update_forecaster', train_model, dbs_connection))
    listen_frontend.start()
    listen_frontend.join()
    dbs_connection.close()



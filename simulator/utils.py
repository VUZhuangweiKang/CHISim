import pandas as pd
from tsmoothie.smoother import SpectralSmoother
from sklearn.preprocessing import MinMaxScaler
import logging
from datetime import datetime
import yaml
import numpy as np
import time
from statsmodels.tsa.holtwinters import ExponentialSmoothing


get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()

def spectral_smoother(df):
    sds = df.values
    smoother = SpectralSmoother(smooth_fraction=0.3, pad_len=1)
    smoother.smooth(sds)
    return smoother

def minmax_scaler(x, max_, min_):
    x = np.array(x)
    return (x - min_) / (max_ - min_)

def minmax_scaler_inverse(x_std, max_, min_):
    x_std = np.array(x_std)
    return x_std * (max_ - min_) + min_

def resample_sum(df, slot):
    rs_sum = pd.DataFrame([])
    rs_sum['start_on'] = pd.to_datetime(df['start_on'])
    rs_sum['node_cnt'] = df.node_cnt
    rs_sum = rs_sum.set_index('start_on', drop=True)
    rs_sum = rs_sum.resample('%dH' % slot).sum().fillna(0)
    rs_sum.reset_index(inplace=True)
    return rs_sum


def data_preprocess(df, slot):
    # step 1. re-sample
    df = resample_sum(df, slot)
    df.dropna(inplace=True)
    df.set_index(['start_on'], inplace=True)
    df = df.astype(float)

    # step 2. smooth data
    smoother = spectral_smoother(df)
    smooth_df = smoother.smooth_data.squeeze()
    df = pd.DataFrame([])
    df['node_cnt'] = smooth_df

    # step 3. data normalization
    # TODO: validate the efficiency of MinMaxScaler
    scaler = MinMaxScaler(feature_range=(0, 1))
    df = scaler.fit_transform(df)

    return df, scaler


def get_logger(logger_name, log_file=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname) -10s %(asctime)s -10s %(funcName) -5s %(lineno) -5d: %(message)s')

    if log_file:
        fl = logging.FileHandler(log_file)
        fl.setLevel(logging.DEBUG)
        fl.setFormatter(formatter)
        logger.addHandler(fl)

    cl = logging.StreamHandler()
    cl.setLevel(logging.DEBUG)
    cl.setFormatter(formatter)
    logger.addHandler(cl)
    return logger


# parse parameters
def load_config(config_file='config.yaml'):
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as er:
            print(er)
            raise

def get_rsrc_mgr_url(config):
    return 'http://%s:5000' % config['framework']['rsrc_mgr']['host']

def get_mongo_url(config):
    return 'mongodb://%s:%s@%s:27017' % (config['simulation']['credential']['username'], config['simulation']['credential']['password'], config['framework']['database']['mongodb'])

def get_scale_ratio(config):
    return config['simulation']['scale_ratio']

def get_influxdb_info(config):
    db_info = config['simulation']['credential']
    db_info.update({'host': config['framework']['database']['influxdb']})
    return db_info

def get_rabbitmq_info(config):
    dbs_info = config['simulation']['credential']
    dbs_info.update({'host': config['framework']['databus']['rabbitmq']})
    return dbs_info

def sleep(sec):
    start = time.time()
    while time.time() - start < sec:
        pass

def scale(X, min_, max_):
    return (X - min_) / (max_ - min_)

def inverse_scale(X_scaled, min_, max_):
    return X_scaled * (max_ - min_) + min_


def exp_smoothing(df, period):
    # create class
    model = ExponentialSmoothing(
        df, 
        seasonal_periods=period, 
        trend="add", 
        seasonal="add", 
        use_boxcox=True, 
        initialization_method="estimated",).fit(smoothing_level=0.2,optimized=True)
    
    return model
import pandas as pd
from tsmoothie.smoother import SpectralSmoother
from sklearn.preprocessing import MinMaxScaler
import logging
from datetime import datetime

get_timestamp = lambda time_str: datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()

def spectral_smoother(df):
    sds = df.values
    smoother = SpectralSmoother(smooth_fraction=0.3, pad_len=1)
    smoother.smooth(sds)
    return smoother


def resample_sum(df, slot):
    rs_sum = pd.DataFrame([])
    rs_sum['start_on'] = pd.to_datetime(df.start_on)
    rs_sum['node_cnt'] = df.node_cnt
    rs_sum = rs_sum.set_index('start_on', drop=True)
    rs_sum = rs_sum.resample('%dH' % slot, closed='right').sum().fillna(0)
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


def get_logger(logger_name, log_file):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    fl = logging.FileHandler(log_file)
    fl.setLevel(logging.DEBUG)

    cl = logging.StreamHandler()
    cl.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(message)s')
    fl.setFormatter(formatter)
    cl.setFormatter(formatter)

    logger.addHandler(fl)
    logger.addHandler(cl)

    return logger
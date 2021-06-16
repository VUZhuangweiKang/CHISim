import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pyanom.outlier_detection import CAD
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
import itertools
import warnings
warnings.filterwarnings("ignore") # specify to ignore warning messages
from statsmodels.tsa.arima_model import ARIMA 
import statsmodels.api as sm
from tsmoothie.smoother import LowessSmoother



def plot_rsrc(data, node_type, show_outlier=False, confidence=0.95):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data.start_on, y=data.node_cnt, mode='lines', name=node_type))
    if show_outlier:
        outliers = data[data['outlier']>confidence]
        fig.add_trace(go.Scatter(x=outliers.start_on, y=outliers.node_cnt, mode='markers', name='outlier'))
    fig.update_layout(showlegend=True)

    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="LightSteelBlue",
    )
    fig.show()


def select_data(file, time_range):
    df = pd.read_csv(file)
    df.start_on = pd.to_datetime(df['start_on'])
    df.sort_values(by=['start_on'], inplace=True)
    df = df[(df.start_on > time_range[0]) & (df.start_on < time_range[1])]
    return df


def cusum_detector(data, threshold, test_size=0.2):
    model = CAD()
    train, test = train_test_split(data, shuffle=False, test_size=test_size)
    model.fit(train.node_cnt, threshold=threshold)
    # results = model.score(test.node_cnt)
    # test['outlier'] = results.squeeze().astype(int)
    results = model.score(data.node_cnt)
    data['outlier'] = results.squeeze().astype(int)
    return data


def resample_sum(data, slot):
    rs_sum = pd.DataFrame([])
    rs_sum['start_on'] = pd.to_datetime(data.start_on)
    rs_sum['node_cnt'] = data.node_cnt
    rs_sum = rs_sum.set_index('start_on', drop=True)
    rs_sum = rs_sum.resample('%dH' % slot).sum().fillna(0)
    rs_sum.reset_index(inplace=True)
    return rs_sum


def get_roll_avg(data, slot):
    roll_avg = pd.DataFrame([])
    roll_avg['start_on'] = pd.to_datetime(data.start_on)
    roll_avg['node_cnt'] = data.rolling(slot).mean()
    return roll_avg


def plot_roll_val(timeseries, rw):
    #Determing rolling statistics:
    rolmean=timeseries['node_cnt'].rolling(window=rw).mean()
    rolstd=timeseries['node_cnt'].rolling(window=rw).std()
    
    fig=plt.figure(figsize=(15,8))
    orig=plt.plot(timeseries['node_cnt'],color='blue',label='Original')
    mean=plt.plot(rolmean,color='red',label='Rolling Mean')
    std=plt.plot(rolstd,color='black',label='Rolling std')
    plt.legend(loc='best')
    plt.title('Rolling Mean & Standard Deviation')
    plt.show()


def test_stationarity(timeseries):
    t = adfuller(timeseries)
    output=pd.DataFrame(index=['Test Statistic Value', "p-value", "Lags Used", "Number of Observations Used","Critical Value(1%)","Critical Value(5%)","Critical Value(10%)"],columns=['value'])
    output['value']['Test Statistic Value'] = t[0]
    output['value']['p-value'] = t[1]
    output['value']['Lags Used'] = t[2]
    output['value']['Number of Observations Used'] = t[3]
    output['value']['Critical Value(1%)'] = t[4]['1%']
    output['value']['Critical Value(5%)'] = t[4]['5%']
    output['value']['Critical Value(10%)'] = t[4]['10%']
    print(output)
    return output


def plotseasonal(res, axes):
    res.observed.plot(ax=axes[0], legend=False)
    axes[0].set_ylabel('Observed')
    res.trend.plot(ax=axes[1], legend=False)
    axes[1].set_ylabel('Trend')
    res.seasonal.plot(ax=axes[2], legend=False)
    axes[2].set_ylabel('Seasonal')
    res.resid.plot(ax=axes[3], legend=False)
    axes[3].set_ylabel('Residual')


def rmse(pred, obs):
    return np.sqrt(sum((pred-obs)**2)/pred.shape[0])


def plot_trace(data, fields):
    fig = go.Figure()
    for f in fields:
        fig.add_trace(go.Scatter(x=data.index, y=data[f], mode='lines', name=f))
    fig.update_layout(showlegend=True)

    fig.update_layout(
        width=1000,
        height=500,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="LightSteelBlue",
    )
    fig.show()


def plot_train_history(history):
    plt.plot(history.history['loss'])
    plt.plot(history.history['val_loss'])
    plt.title('model loss')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train', 'val'], loc='best')
    plt.show()


"""
LowessSmoother uses LOWESS (locally-weighted scatterplot smoothing)
to smooth the timeseries. This smoothing technique is a non-parametric
regression method that essentially fit a unique linear regression
for every data point by including nearby data points to estimate
the slope and intercept. The presented method is robust because it
performs residual-based reweightings simply specifing the number of
iterations to operate.
"""
def lowess_smoother(df):
    """
    :param df: a pandas dataframe
    :return: smoother object
    """
    sds = df.values
    # set the smooth_fraction to 1 day
    days = df.index[-1] - df.index[0]
    frac = 1 / days.days
    smoother = LowessSmoother(smooth_fraction=frac, iterations=1)
    smoother.smooth(sds)
    return smoother
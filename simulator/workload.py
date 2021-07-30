import time
import databus as dbs
from utils import *
import threading
from multiprocessing import Pool


if __name__ == '__main__':
    config = load_config()
    scale_ratio = get_scale_ratio(config)

    pool = Pool()

    me_args = config['workloads']['machine_events']
    me_args.update({
        'scale_ratio': scale_ratio,
        'exchange': 'machine_events_exchange', 
        'routing_key': 'machine_event'})
    pool.apply_async(dbs.emit_timeseries, kwds=me_args)

    cr_args = config['workloads']['chameleon_requests']
    cr_args.update({
        'scale_ratio': scale_ratio,
        'exchange': 'user_requests_exchange', 
        'routing_key': 'raw_request'})
    pool.apply_async(dbs.emit_timeseries, kwds=cr_args)

    oj_thread = None
    if config['simulation']['enable_osg']:
        oj_args = config['workloads']['osg_jobs']
        oj_args.update({
            'scale_ratio': scale_ratio,
            'exchange': 'osg_jobs_exchange', 
            'routing_key': 'osg_job'})
        pool.apply_async(dbs.emit_timeseries, kwds=oj_args)

    while True:
        pass
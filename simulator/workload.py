import databus as dbs
from utils import *
import threading


if __name__ == '__main__':
    config = load_config()
    scale_ratio = get_scale_ratio(config)

    me_args = config['workloads']['machine_events']
    me_args.update({
        'scale_ratio': scale_ratio,
        'exchange': 'machine_events_exchange', 
        'routing_key': 'machine_event'})
    me_thread = threading.Thread(target=dbs.emit_timeseries, kwargs=me_args, daemon=True)

    cr_args = config['workloads']['chameleon_requests']
    cr_args.update({
        'scale_ratio': scale_ratio,
        'exchange': 'user_requests_exchange', 
        'routing_key': 'raw_request'})
    cr_thread = threading.Thread(target=dbs.emit_timeseries, kwargs=cr_args, daemon=True)

    oj_thread = None
    if config['simulation']['enable_osg']:
        oj_args = config['workloads']['osg_jobs']
        oj_args.update({
            'scale_ratio': scale_ratio,
            'exchange': 'osg_jobs_exchange', 
            'routing_key': 'osg_job'})
        oj_thread = threading.Thread(target=dbs.emit_timeseries, kwargs=oj_args, daemon=True)
    
    me_thread.start()
    cr_thread.start()
    if oj_thread:
        oj_thread.start()
    
    me_thread.join()
    cr_thread.join()
    if oj_thread:
        oj_thread.join()
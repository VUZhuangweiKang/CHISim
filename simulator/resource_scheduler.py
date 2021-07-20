import databus as dbs
import requests
import argparse
from abc import ABCMeta, abstractmethod
import pickle
import threading
from utils import *


def handle_osg(self, ch, method, properties, body):
    # TODO: deploy free nodes to OSG
    pass


def handle_chameleon(ch, method, properties, body):
    if properties.headers['key'] != 'schedule_resource':
        return
    pred_requests = pickle.loads(body)
    for index, row in pred_requests.iterrows():
        rv = requests.post(url='%s:5000/acquire_nodes' % rsrc_mgr,
                           json={'node_type': row['node_type'], 'node_cnt': row['node_cnt'], 'pool': 'Chameleon'})
        if rv.status_code == 200:
            logger.info(msg='Acquire %d %s nodes successfully' % (row['node_cnt'], row['node_type']))
        elif rv.status_code == 202:
            logger.info(
                msg='Available nodes cannot satisfy predicted requests, %s more nodes are needed.' % rv.text)
            terminate_nodes = terminator(row['node_type'], row['node_cnt'])
            body = {"node_type": row['node_type'], "preempt_nodes": terminate_nodes}
            requests.post(url='%s:5000/preempt_nodes' % rsrc_mgr, json=body)


class ResourceScheduler:
    @abstractmethod
    def terminator(self, node_type, node_cnt):
        pass

    def get_osg_info(self):
        pass

    def main(self):
        handle_osg_thr = threading.Thread(target=dbs.consume, args=('osg_jobs_exchange', 'osg_jobs_queue', 'osg_job', handle_osg))
        handle_osg_thr.start()
        dbs.consume('internal_exchange', 'internal_queue', 'schedule_resource', handle_chameleon)
        handle_osg_thr.join()


class RandomScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def terminator(self, node_type, node_cnt):
        pass


class MostRecentDeployScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def terminator(self, node_type, node_cnt):
        pass


class LeastResourceUtlScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def terminator(self, node_type, node_cnt):
        pass


class HybridScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def terminator(self, node_type, node_cnt):
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scheduler', type=str, choices=['RandomScheduler', 'MostRecentDeployScheduler', 'LeastResourceUtlScheduler', 'HybridScheduler'], default='RandomScheduler', help='select a scheduler')
    parser.add_argument('--rsrc_mgr', type=str, help='IP of the resource manager')
    args = parser.parse_args()

    rsrc_mgr = args.rsrc_mgr
    logger = get_logger('resource_scheduler', 'resource_scheduler.log')
    scheduler = args.scheduler
    if scheduler == 'MostRecentDeployScheduler':
        rsch = MostRecentDeployScheduler(rsrc_mgr_url=args.rsrc_mgr)
    elif scheduler == 'LeastResourceUtlScheduler':
        rsch = LeastResourceUtlScheduler(rsrc_mgr_url=args.rsrc_mgr)
    elif scheduler == 'HybridScheduler':
        rsch = LeastResourceUtlScheduler(rsrc_mgr_url=args.rsrc_mgr)
    else:
        rsch = RandomScheduler(rsrc_mgr_url=args.rsrc_mgr)
    rsch.main()

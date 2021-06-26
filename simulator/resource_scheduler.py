import databus as dbs
import requests
import argparse
from abc import ABCMeta, abstractmethod
import pickle
from utils import *


class ResourceScheduler:
    def __init__(self, rsrc_mgr_url):
        self.dbs_connection = dbs.init_connection()
        self.rsrc_mgr = rsrc_mgr_url
        self.logger = get_logger('resource_scheduler', 'resource_scheduler.log')

    @abstractmethod
    def scheduler(self):
        pass

    def get_osg_info(self):
        pass

    def schedule_resource(self, ch, method, properties, body):
        if properties['header']['key'] != 'schedule_resource':
            return
        pred_requests = pickle.loads(body)
        for index, row in pred_requests.iterrows():
            rv = requests.post(url='%s:5000/acquire_nodes' % self.rsrc_mgr,
                               json={'node_type': row['node_type'], 'node_cnt': row['node_cnt']})
            if rv.status_code == 200:
                self.logger.info(msg='Acquire %d %s nodes successfully' % (row['node_cnt'], row['node_type']))
            elif rv.status_code == 201:
                self.logger.info(
                    msg='Available nodes cannot satisfy predicted requests, %s nodes are needed.' % rv.text)
                # TODO: add preemption operations
            elif rv.status_code == 403:
                self.logger.error(msg='Failed to acquire nodes from the resource pool.')

    def main(self):
        dbs.consume('resource_scheduler', 'internal', 'schedule_resource', ResourceScheduler.schedule_resource,
                    self.dbs_connection)


class RandomScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def scheduler(self):
        pass


class MostRecentScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def scheduler(self):
        pass


class LeastUseResourceScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def scheduler(self):
        pass


class HybridScheduler(ResourceScheduler):
    def __init__(self, rsrc_mgr_url):
        super().__init__(rsrc_mgr_url)

    def scheduler(self):
        pass


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--rsrc_mgr', type=str, help='IP of the resource manager')
#     args = parser.parse_args()
#     rsch = ResourceScheduler(rsrc_mgr_url=args.rsrc_mgr)
#     rsch.main()

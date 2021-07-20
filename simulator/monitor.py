from influxdb import InfluxDBClient
import pymongo
import json
from utils import *


class Monitor:
    def __init__(self):
        config = load_config()
        self.influx_client = InfluxDBClient(**get_influxdb_info(config), database='ChameleonSimulator')

        mongo_client = pymongo.MongoClient(get_mongo_url(config))
        mongodb = mongo_client['ChameleonSimulator']
        self.resource_pool = mongodb['resource_pool']
        self.osg_jobs = mongodb['osg_jobs']
        self.ch_leases = mongodb['chameleon_leases']

    def measure_rsrc(self):
        # monitor resource pool
        ch_inuse = self.resource_pool.count({"$and": [{"status": "inuse"}, {"pool": "chameleon"}]})
        ch_free =  self.resource_pool.count({"$and": [{"status": "free"}, {"pool": "chameleon"}]})
        osg_inuse =  self.resource_pool.count({"$and": [{"status": "inuse"}, {"pool": "osg"}]})
        imp = 100 * osg_inuse / (ch_inuse + ch_free + osg_inuse)
        json_body = [{'measurement': 'resource_pool', 'fields': {
            'chi-inuse': ch_inuse, 
            'ch-free': ch_free, 
            'osg-inuse': osg_inuse,
            'imp(%)': imp
        }}]
        self.influx_client.write_points(json_body)
    
    def monitor_osg_jobs(self, runnings, pendings, completed, terminated):
        json_body = [{'measurement': 'osg_jobs', 'fields': {'running': runnings, 'pending': pendings, 'completed': completed, 'terminated': terminated}}]
        self.influx_client.write_points(json_body)

    def monitor_chameleon(self, completed):
        self.influx_client.write_points([{'measurement': "chameleon_leases", 'fields': {'completed': completed}}])
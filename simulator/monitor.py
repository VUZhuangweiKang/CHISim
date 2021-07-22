from influxdb import InfluxDBClient
import pymongo
import json
import pandas as pd
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
        agg_body = [
            {"$match": ""},
            {"$project": {"status": "$status", "pool": "$pool"}}
        ]
        rp = self.resource_pool.find({},{'status': 1, 'pool': 1})
        rp = pd.DataFrame(list(rp))
        ch_inuse = rp[(rp['status']=='inuse') & (rp['pool']=='chameleon')].shape[0]
        ch_free = rp[(rp['status']=='free') & (rp['pool']=='chameleon')].shape[0]
        osg_inuse = rp[(rp['status']=='inuse') & (rp['pool']=='osg')].shape[0]
        ch_utl = 100*ch_inuse/(ch_inuse + ch_free + osg_inuse)
        osg_utl = 100*osg_inuse/(ch_inuse + ch_free + osg_inuse)
        total_utl = ch_utl + osg_utl
        json_body = [{'measurement': 'resource_pool', 'fields': {
            'chi-inuse': ch_inuse, 
            'ch-free': ch_free, 
            'osg-inuse': osg_inuse,
            'ch_utl_rate': ch_utl,
            'osg_utl_rate': osg_utl,
            'total_utl_rate': total_utl
        }}]
        self.influx_client.write_points(json_body)
    
    def monitor_osg_jobs(self, runnings, pendings, completed, terminated):
        json_body = [{'measurement': 'osg_jobs', 'fields': {'running': runnings, 'pending': pendings, 'completed': completed, 'terminated': terminated}}]
        self.influx_client.write_points(json_body)

    def monitor_chameleon(self, completed):
        self.influx_client.write_points([{'measurement': "chameleon_leases", 'fields': {'completed': completed}}])
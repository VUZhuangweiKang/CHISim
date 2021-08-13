import time
import databus as dbs
import influxdb
import pymongo
from pika.exchange_type import ExchangeType
from utils import *


if __name__ == '__main__':
    config = load_config()
    if config['framework']['global_mgr']['clean_run']:
        db_client = influxdb.InfluxDBClient(**get_influxdb_info(config))
        db_list = [db['name'] for db in db_client.get_list_database()]
        if 'ChameleonSimulator' in db_list:
            db_client.drop_database('ChameleonSimulator')
        db_client.create_database('ChameleonSimulator')

        mongo_client = pymongo.MongoClient(get_mongo_url(config))
        mongo_client.drop_database('ChameleonSimulator')

        dbs_connection = dbs.init_connection()
        gm_channel = dbs_connection.channel()
        dbs.clear_databus(gm_channel)

        exchanges = ['internal_exchange', 'user_requests_exchange', 'osg_jobs_exchange', 'machine_events_exchange']
        for ex in exchanges:
            gm_channel.exchange_declare(ex, exchange_type=ExchangeType.topic)
        
        queues = ['internal_queue', 'user_requests_queue', 'osg_jobs_queue', 'machine_events_queue']
        for q in queues:
            gm_channel.queue_declare(q, durable=True)
        
    # send hearbeats periodically to maintain activiness of consumer connections
    while True:
        time.sleep(3)

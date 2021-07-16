import time
import databus as dbs
import json
import influxdb
import pymongo


if __name__ == '__main__':
    with open('influxdb.json') as f:
        db_info = json.load(f)
    db_client = influxdb.InfluxDBClient(**db_info)
    db_list = [db['name'] for db in db_client.get_list_database()]
    if 'ChameleonSimulator' in db_list:
        db_client.drop_database('ChameleonSimulator')
    db_client.create_database('ChameleonSimulator')

    mongo_client = pymongo.MongoClient('mongodb://chi-sim:chi-sim@127.0.0.1:27017')
    mongo_client.drop_database('ChameleonSimulator')

    dbs_connection = dbs.init_connection()
    gm_channel = dbs_connection.channel()
    dbs.clear_databus(gm_channel)

    gm_channel.exchange_declare('internal_exchange')
    gm_channel.exchange_declare('user_requests_exchange')
    gm_channel.exchange_declare('machine_events_exchange')
    gm_channel.exchange_declare('osg_jobs_exchange')
    gm_channel.queue_declare(queue='user_requests_queue')
    gm_channel.queue_declare(queue='machine_events_queue')
    gm_channel.queue_declare(queue='osg_jobs_queue')
    gm_channel.queue_declare(queue='internal_queue')
    
    # send hearbeats periodically to maintain activiness of consumer connections
    while True:
        time.sleep(3)

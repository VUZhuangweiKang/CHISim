import time
import json
import influxdb
import pymongo
from AsyncDatabus.Publisher import Publisher


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

    # async amqp connection
    dbs_client = Publisher('amqp://chi-sim:chi-sim@localhost:5672/%2F?connection_attempts=3&heartbeat=3600')
    dbs_client.run()
    dbs_client.open_channel()
    while not dbs_client.channel.is_open:
        pass

    ex_queue_pair = [('internal_exchange', 'internal_queue'), ('user_requests_exchange', 'user_requests_queue'), ('machine_events_exchange', 'machine_events_queue'), ('osg_jobs_exchange', 'osg_jobs_queue')]
    for item in ex_queue_pair:
        dbs_client.setup_exchange(item[0])
        dbs_client.setup_queue(item[1])

    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        dbs_client.stop()

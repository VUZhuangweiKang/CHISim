import json
import functools
from pika import exchange_type
from AsyncDatabus.AsyncClient import AsyncClient
import influxdb
import pymongo
from AsyncDatabus.Publisher import Publisher
from pika.exchange_type import ExchangeType


class GlobalManager(AsyncClient):
    def __init__(self, amqpurl):
        super().__init__(amqpurl)
        self.influx_client = None
        self.mongo_client = None
    
    def setup_db(self, influx_info, mongourl):
        self.influx_client = influxdb.InfluxDBClient(**influx_info)
        db_list = [db['name'] for db in self.influx_client.get_list_database()]
        if 'ChameleonSimulator' in db_list:
            self.influx_client.drop_database('ChameleonSimulator')
        self.influx_client.create_database('ChameleonSimulator')

        self.mongo_client = pymongo.MongoClient(mongourl)
        self.mongo_client.drop_database('ChameleonSimulator')

    def setup_exchange(self):
        exchanges = ['internal_exchange', 'user_requests_exchange', 'machine_events_exchange', 'osg_jobs_exchange']
        for exchange in exchanges:
            cb = functools.partial(
                self.on_exchange_declareok, userdata=exchange)
            self.channel.exchange_declare(
                exchange=exchange,
                exchange_type=ExchangeType.direct,
                callback=cb)
    
    def setup_queue(self):
        queues = ['internal_queue', 'user_requests_queue', 'machine_events_queue', 'osg_jobs_queue']
        for q in queues:
            cb = functools.partial(self.on_queue_declareok, userdata=q)
            self.channel.queue_declare(queue=q, callback=cb)
    
    def clean_amqp(self):
        exchanges = ['internal_exchange', 'user_requests_exchange', 'machine_events_exchange', 'osg_jobs_exchange']
        for ex in exchanges:
            self.channel.exchange_delete(ex)
        queues = ['internal_queue', 'user_requests_queue', 'machine_events_queue', 'osg_jobs_queue']
        for q in queues:
            self.channel.queue_delete(q)


if __name__ == '__main__':
    with open('influxdb.json') as f:
        influx_info = json.load(f)
    
    gm = GlobalManager(amqpurl='amqp://chi-sim:chi-sim@127.0.0.1:5672/%2F?connection_attempts=3&heartbeat=0')
    gm.setup_db(influx_info, 'mongodb://chi-sim:chi-sim@127.0.0.1:27017')
    gm.run()
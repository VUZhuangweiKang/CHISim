# -*- coding: utf-8 -*-
import pika
import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class SyncClient(object):
    def __init__(self, connect_info, exchange, exchange_type, queue, routing_key):
        self.connect_info = connect_info
        self.connection = None
        self.channel = None

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.bind_ok = False
        self.routing_key = routing_key

    def connect(self):
        params = (pika.ConnectionParameters(
            host=self.connect_info['host'],
            heartbeat=0,
            credentials=pika.credentials.PlainCredentials(username=self.connect_info['username'],
                                                          password=self.connect_info['password'], erase_on_connect=True))
        )
        self.connection = pika.BlockingConnection(parameters=params)
        while not self.connection.is_open:
            pass

    def open_channel(self):
        self.channel = self.connection.channel()
        while self.channel.is_open():
            pass

    def clear_databus(self):
        self.channel.exchange_delete('internal_exchange')
        self.channel.exchange_delete('user_requests_exchange')
        self.channel.exchange_delete('machine_events_exchange')
        self.channel.exchange_delete('osg_jobs_exchange')

        self.channel.queue_delete('user_requests_queue')
        self.channel.queue_delete('machine_events_queue')
        self.channel.queue_delete('internal_queue')
        self.channel.queue_delete('osg_jobs_queue')

    def close(self):
        self.clear_databus()
        self.channel.close()
        self.connection.close()


# -*- coding: utf-8 -*-
import functools
import logging
import pika
from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class AsyncClient(object):
    def __init__(self, amqpurl):
        self.url = amqpurl
        self.connection = None
        self.channel = None
        self.stopping = False

    def connect(self):
        LOGGER.info('Connecting to %s', self.url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self.url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_closed(self, _unused_connection, reason):
        self.channel = None
        if self.stopping:
            self.connection.ioloop.stop()

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self.connection.ioloop.call_later(5, self.connection.ioloop.stop)

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def setup_exchange(self, exchange):
        LOGGER.info('Declaring exchange: %s', exchange)
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange)
        self.channel.exchange_declare(
            exchange=exchange,
            exchange_type=ExchangeType.topic,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self.channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        LOGGER.info('Queue declared: %s', queue_name)

    def bind_queue(self, exchange_name, queue_name, routing_key):
        cb = functools.partial(self.on_queue_bindok, userdata=queue_name)
        self.channel.queue_bind(
            queue_name,
            exchange_name,
            routing_key=routing_key,
            callback=cb)

    def on_queue_bindok(self, _unused_frame, userdata):
        LOGGER.info('Queue bound')

    def close_channel(self):
        if self.channel is not None:
            LOGGER.info('Closing the channel')
            self.channel.close()

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)

    def close_connection(self):
        if self.connection.is_closing or self.connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self.connection.close()

    def run(self):
        try:
            self.connection = self.connect()
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()
            if (self.connection is not None and
                    not self.connection.is_closed):
                self.connection.ioloop.start()
        LOGGER.info('Stopped')

    def stop(self):
        pass
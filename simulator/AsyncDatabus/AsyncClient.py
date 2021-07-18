# -*- coding: utf-8 -*-
import functools
import logging
import pika
from pika.exchange_type import ExchangeType
from utils import get_logger

LOGGER = get_logger(__name__)


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
        self.clean_amqp()
        self.setup_exchange()

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def setup_exchange(self):
        pass

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue()

    def setup_queue(self):
        pass

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        LOGGER.info('Queue declared: %s', queue_name)
        self.bind_queue()

    def bind_queue(self):
        pass

    def on_queue_bindok(self, _unused_frame, userdata):
        pass

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
            if (self.connection is not None and not self.connection.is_closed):
                self.connection.ioloop.start()
        LOGGER.info('Stopped')

    def clean_amqp(self):
        pass

    def stop(self):
        pass
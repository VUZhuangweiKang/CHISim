# -*- coding: utf-8 -*-
import pandas as pd
from AsyncDatabus.AsyncClient import *


class Publisher(AsyncClient):
    def __init__(self, amqp_url):
        super().__init__(amqp_url)
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

    def emit_msg(self, msg, exchange, routing_key):
        self.enable_delivery_confirmations()
        if self.channel is None or not self.channel.is_open:
            return
        self.channel.basic_publish(exchange, routing_key, msg,
                                   properties=pika.BasicProperties(delivery_mode=1, content_type='application/json', headers={'key': routing_key}))
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info('Published message # %i', self._message_number)

    def emit_timeseries(self, datafile, index_col, scale_ratio, exchange, routing_key):
        self.enable_delivery_confirmations()
        last_send_date = None
        for df in pd.read_csv(datafile, iterator=True, chunksize=100):
            index_col_name = df.columns[index_col]
            for index, row in df.iterrows():
                if last_send_date:
                    sleep_sec = (pd.to_datetime(row[index_col_name]) - pd.to_datetime(last_send_date)).total_seconds() / scale_ratio
                else:
                    sleep_sec = 0
                self.connection.ioloop.call_later(sleep_sec, self.emit_msg, row.to_json(), exchange, routing_key)
                last_send_date = row[index_col_name]

    def enable_delivery_confirmations(self):
        LOGGER.info('Issuing Confirm.Select RPC command')
        self.channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info('Received %s for delivery tag: %i', confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)

    def stop(self):
        LOGGER.info('Stopping')
        self.stopping = True
        self.close_channel()
        self.close_connection()
# -*- coding: utf-8 -*-
from SyncDatabus.SyncClient import *
from pika.exchange_type import ExchangeType
import pandas as pd
import time


class SyncPublisher(SyncClient):
    def __init__(self, connect_info, exchange, exchange_type, queue, routing_key):
        super().__init__(connect_info, exchange, exchange_type, queue, routing_key)
        self.connect()
        self.open_channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.direct)

    def emit_msg(self, payload):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=payload,
            properties=pika.BasicProperties(delivery_mode=1, headers={'key': self.routing_key})
        )

    def emit_timeseries(self, datafile, index_col, scale_ratio):
        last_send_date = None
        try:
            for df in pd.read_csv(datafile, iterator=True, chunksize=100):
                index_col_name = df.columns[index_col]
                for index, row in df.iterrows():
                    if last_send_date:
                        sleep_sec = (pd.to_datetime(row[index_col_name]) - pd.to_datetime(last_send_date)).total_seconds() / scale_ratio
                        time.sleep(sleep_sec)
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.routing_key,
                        body=row.to_json(),
                        properties=pika.BasicProperties(delivery_mode=1, headers={'key': self.routing_key})
                    )
                    last_send_date = row[index_col_name]
        except KeyboardInterrupt:
            pass
        finally:
            self.close()


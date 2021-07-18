import functools
from AsyncDatabus.AsyncClient import AsyncClient, LOGGER
from pika.exchange_type import ExchangeType
import pandas as pd


class WorkloadStream(AsyncClient):
    def __init__(self, amqpurl, datafile, index_col, scale_ratio, exchange, queue, routing_key):
        super().__init__(amqpurl)

        self.datafile = datafile
        self.index_col = index_col
        self.scale_ratio = scale_ratio

        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key
    
    def setup_exchange(self):
        cb = functools.partial(self.on_exchange_declareok, userdata=self.exchange)
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=ExchangeType.direct,
            callback=cb)
    
    def setup_queue(self):
        cb = functools.partial(self.on_queue_declareok, userdata=self.queue)
        self.channel.queue_declare(queue=self.queue, callback=cb)
    
    def bind_queue(self):
        LOGGER.info('Binding %s to %s with %s', self.exchange, self.queue, self.routing_key)
        self._channel.queue_bind(
            self.queue,
            self.exchange,
            routing_key=self.routing_key,
            callback=self.on_queue_bindok)

    def on_queue_bindok(self, _unused_frame, userdata):
        LOGGER.info('Queue bound %s ' % userdata)
        self.start_publishing()

    def start_publishing(self):
        self.emit_timeseries()

    def emit_timeseries(self):
        self.enable_delivery_confirmations()
        last_send_date = None
        for df in pd.read_csv(self.datafile, iterator=True, chunksize=100):
            index_col_name = df.columns[self.index_col]
            for index, row in df.iterrows():
                if last_send_date:
                    sleep_sec = (pd.to_datetime(row[index_col_name]) - pd.to_datetime(last_send_date)).total_seconds() / self.scale_ratio
                else:
                    sleep_sec = 0
                self.connection.ioloop.call_later(sleep_sec, self.emit_msg, row.to_json(), self.exchange, self.routing_key)
                last_send_date = row[index_col_name]

# -*- coding: utf-8 -*-
from SyncDatabus.SyncClient import *
from pika.exchange_type import ExchangeType


class SyncConsumer(SyncClient):
    def __init__(self, connect_info, exchange, exchange_type, queue, routing_key):
        super().__init__(connect_info, exchange, exchange_type, queue, routing_key)
        self.connect()
        self.open_channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.direct)

    def consume(self, callback):
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=callback, auto_ack=True)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.stop_consume()
            self.close()

    def stop_consume(self):
        self.channel.stop_consuming()
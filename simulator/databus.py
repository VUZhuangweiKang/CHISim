import pandas as pd
import time
import os
import json
import pika
from datetime import datetime
from pika.exchange_type import ExchangeType
from requests.api import head
from utils import *


def init_connection():
    config = load_config()
    connect_info = get_rabbitmq_info(config)
    params = (pika.ConnectionParameters(
        host=connect_info['host'],
        heartbeat=0,
        blocked_connection_timeout=0,
        credentials=pika.credentials.PlainCredentials(username=connect_info['username'], password=connect_info['password'], erase_on_connect=True),
        connection_attempts=10, retry_delay=1)
    )

    connection = pika.BlockingConnection(parameters=params)
    return connection


def emit_msg(exchange, routing_key, payload, channel):
    channel.exchange_declare(exchange=exchange, exchange_type=ExchangeType.topic)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=payload,
        properties=pika.BasicProperties(headers={'key': routing_key}),
        mandatory=True
    )


def emit_timeseries(exchange, routing_key, payload, index_col, scale_ratio):
    connection = init_connection()
    channel = connection.channel()
    channel.confirm_delivery()
    channel.exchange_declare(exchange, exchange_type=ExchangeType.topic)
    last_send_at = None
    for df in pd.read_csv(payload, iterator=True, chunksize=1000):
        index_col_name = df.columns[index_col]
        for index, row in df.iterrows():
            if last_send_at:
                sleep_sec = (pd.to_datetime(row[index_col_name]) - pd.to_datetime(last_send_at)).total_seconds()/scale_ratio
                time.sleep(sleep_sec)
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=row.to_json(),
                properties=pika.BasicProperties(delivery_mode=1, headers={'key': routing_key}),
                mandatory=True
            )
            last_send_at = row[index_col_name]
    print('%s done' % payload)
    try:
        while True:
            pass
    except KeyboardInterrupt:
        connection.close()


def consume(exchange, queue, binding_key, callback, consume_type='push'):
    connection = init_connection()
    channel = connection.channel()
    channel.queue_bind(exchange=exchange, queue=queue, routing_key=binding_key)
    try:
        if consume_type == 'push':
            channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        elif consume_type == 'pull':
            for method, header, body in channel.consume(queue):
                if method:
                    channel.basic_ack(method.delivery_tag)
                    callback(method, header, body)
    except KeyboardInterrupt as ex:
        if consume_type == 'push':
            channel.stop_consuming()
        else:
            channel.cancel()
    channel.close()
    connection.close()


def clear_databus(channel):
    channel.exchange_delete('internal_exchange')
    channel.exchange_delete('user_requests_exchange')
    channel.exchange_delete('machine_events_exchange')
    channel.exchange_delete('osg_jobs_exchange')

    channel.queue_delete('user_requests_queue')
    channel.queue_delete('machine_events_queue')
    channel.queue_delete('internal_queue')
    channel.queue_delete('osg_jobs_queue')
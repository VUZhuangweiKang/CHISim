import pandas as pd
import time
import os
import json
import pika
from pika.exchange_type import ExchangeType


def init_connection():
    assert os.path.exists('rabbitmq.json')
    with open('rabbitmq.json') as f:
        connect_info = json.load(f)

    params = (pika.ConnectionParameters(
        host=connect_info['host'],
        heartbeat=600,
        blocked_connection_timeout=3000,
        credentials=pika.credentials.PlainCredentials(username=connect_info['username'], password=connect_info['password'], erase_on_connect=True),
        connection_attempts=5, retry_delay=1)
    )

    connection = pika.BlockingConnection(parameters=params)
    return connection


def emit_msg(exchange, routing_key, payload, channel, no_print=False):
    channel.exchange_declare(exchange=exchange, exchange_type=ExchangeType.direct)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=payload,
        properties=pika.BasicProperties(delivery_mode=1, headers={'key': routing_key})
    )
    if not no_print:
        print(payload)


def emit_timeseries(exchange, routing_key, payload, index_col, scale_ratio, no_print=False):
    connection = init_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange)
    last_send_at = None
    for df in pd.read_csv(payload, iterator=True, chunksize=1000):
        index_col_name = df.columns[index_col]
        for index, row in df.iterrows():
            if last_send_at:
                sleep_sec = (pd.to_datetime(row[index_col_name]) - pd.to_datetime(last_send_at)).total_seconds()
                time.sleep(sleep_sec/scale_ratio)
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=row.to_json(),
                properties=pika.BasicProperties(delivery_mode=1, headers={'key': routing_key})
            )
            last_send_at = row[index_col_name]
            if not no_print:
               print(row.to_json())
    connection.close()


def consume(exchange, queue, binding_key, callback):
    connection = init_connection()
    channel = connection.channel()
    channel.queue_bind(exchange=exchange, queue=queue, routing_key=binding_key)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


def clear_databus(channel):
    channel.exchange_delete('internal_exchange')
    channel.exchange_delete('user_requests_exchange')
    channel.exchange_delete('machine_events_exchange')

    channel.queue_delete('user_requests_queue')
    channel.queue_delete('machine_events_queue')
    channel.queue_delete('internal_queue')
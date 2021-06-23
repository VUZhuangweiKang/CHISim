import pandas as pd
import time
import signal
import os, json
import pika
from pika.exchange_type import ExchangeType


def init_connection():
    assert os.path.exists('rabbitmq.json')
    with open('rabbitmq.json') as f:
        connect_info = json.load(f)

    params = (pika.ConnectionParameters(
        host=connect_info.host,
        credentials=pika.credentials.PlainCredentials(username=connect_info.username, password=connect_info.password,
                                                      erase_on_connect=True),
        connection_attempts=5, retry_delay=1)
    )

    connection = pika.BlockingConnection(parameters=params)
    return connection


def emit_msg(exchange, routing_key, payload, connection=None, channel=None, close_connection=False):
    if not connection:
        connection = init_connection()
    if not channel:
        channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type=ExchangeType.direct)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=payload.encode(),
        properties=pika.BasicProperties(content_type='application/json', delivery_mode=1)
    )
    if close_connection:
        connection.close()


def emit_timeseries(exchange, routing_key, payload, index_col, scale_ratio=1, connection=None, channel=None):
    if not connection:
        connection = init_connection()
    if not channel:
        channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type=ExchangeType.direct)
    for df in pd.read_csv(payload, iterator=True, chunksize=1000):
        df[index_col] = pd.to_datetime(df[index_col])
        df.set_index(index_col, inplace=True)
        pre_timestamp = pd.Timestamp.now()
        for index, row in df.iterrows():
            message_body = row.to_json()
            sleep_sec = (message_body.index - pre_timestamp).total_seconds()
            time.sleep(sleep_sec / scale_ratio)
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(message_body).encode(),
                properties=pika.BasicProperties(content_type='application/json', delivery_mode=1)
            )
    connection.close()


def queue_bind(channel, exchange, queues):
    for q in queues:
        channel.queue_bind(exchange=exchange, queue=q)


def consume(exchange, queue, binding_key, callback, connection=None, channel=None):
    if not connection:
        connection = init_connection()
    if not channel:
        channel = connection.channel()
    result = channel.queue_declare(queue=queue, exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=binding_key)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    def handle_termination():
        channel.stop_consuming()
        connection.close()

    signal.signal(signal.SIGINT, handle_termination)
    signal.signal(signal.SIGTERM, handle_termination)
    channel.start_consuming()



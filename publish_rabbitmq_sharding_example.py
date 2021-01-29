import json
import os
from enum import Enum
import random

import pika


class QueueEnum(Enum):
    CONVERSATION_DATA_RECOVERY = {'route': 'conversation_analytics_data_recovery',
                                  'queue': 'conversation_analytics_data_recovery_q',
                                  'exchange_type': 'direct', 'exchange_name': 'wotnot.direct'}


def rabbit_mq_connect():
    parameters = pika.URLParameters(RABBIT_MQ_URL_PARAMETER)
    connection = pika.BlockingConnection(parameters)
    return connection


def rabbit_mq_close(connection):
    if connection is not None:
        connection.close()


class RabbitMqConnection(object):
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = rabbit_mq_connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        rabbit_mq_close(self.conn)


def publish_message(route_key, payload, exchange_type='direct', delivery_mode=2, exchange_name='wotnot.direct'):
    with RabbitMqConnection() as conn:
        channel = conn.channel()
        channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        channel.basic_publish(exchange=exchange_name,
                              routing_key=route_key,
                              body=json.dumps(payload, ensure_ascii=False),
                              properties=pika.BasicProperties(
                                  delivery_mode=delivery_mode,
                              ))


def create_queues():
    with RabbitMqConnection() as conn:
        for data in QueueEnum:
            channel = conn.channel()
            channel.exchange_declare(exchange=data.value['exchange_name'],
                                     exchange_type=data.value['exchange_type'], durable=True)
            channel.queue_declare(queue=data.value['queue'], durable=True)
            channel.queue_bind(exchange=data.value['exchange_name'], routing_key=data.value['route'],
                               queue=data.value['queue'])

def do_process():
    for i in range(10):
        n = random.randint(0, 100)
        publish_message("conversation_key_{}".format(n%3), {"conversation_key": "conversation_key_{}".format(n%3)},
                        "x-modulus-hash",
                        exchange_name="test1")


if __name__ == '__main__':
    # Rabbit Mq
    _AMQP_URL = 'amqp://{user_name}:{password}@{host}:{port}/{vhost}?heartbeat=60&retry_delay=5&connection_attempts=3'

    # RABBIT_MQ_HOST = input("Enter Rabbit Mq Host:") or os.environ.get("RABBIT_MQ_HOST", "127.0.0.1")
    # RABBIT_MQ_PORT = int(input("Enter Rabbit Mq Port:") or os.environ.get("RABBIT_MQ_PORT", 5672))
    # RABBIT_MQ_USERNAME = input("Enter Rabbit Mq Username:") or os.environ.get("RABBIT_MQ_USERNAME", "guest")
    # RABBIT_MQ_PASSWORD = input("Enter Rabbit Mq Password:") or os.environ.get("RABBIT_MQ_PASSWORD", "guest")
    RABBIT_MQ_HOST = "127.0.0.1"
    RABBIT_MQ_PORT = 5672
    RABBIT_MQ_USERNAME = "guest"
    RABBIT_MQ_PASSWORD = "guest"
    RABBIT_MQ_URL_PARAMETER = _AMQP_URL.format(user_name=RABBIT_MQ_USERNAME,
                                               password=RABBIT_MQ_PASSWORD,
                                               host=RABBIT_MQ_HOST,
                                               vhost="%2f",
                                               port=RABBIT_MQ_PORT)
    create_queues()
    do_process()

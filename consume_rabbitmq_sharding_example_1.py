import json
import os
from enum import Enum
from threading import Thread
import time

import pika
from flask import Flask
from pika.exceptions import AMQPError


def create_queues():
    with RabbitMqConnection() as conn:
        for data in QueueEnum:
            channel = conn.channel()
            channel.exchange_declare(exchange=data.value['exchange_name'],
                                     exchange_type=data.value['exchange_type'], durable=True)
            channel.queue_declare(queue=data.value['queue'], durable=True)
            channel.queue_bind(exchange=data.value['exchange_name'], routing_key=data.value['route'],
                               queue=data.value['queue'])


# Rabbit MQ Operations
class QueueEnum(Enum):
    CONVERSATION_DATA_RECOVERY = {'route': 'conversation_analytics_data_recovery',
                                  'queue': 'conversation_analytics_data_recovery_q',
                                  'exchange_type': 'direct', 'exchange_name': 'wotnot.direct',
                                  'worker_count': 1}
    FAILED_CONVERSATION_DATA_RECOVERY = {'route': 'failed_conversation_analytics_data_recovery',
                                         'queue': 'failed_conversation_analytics_data_recovery_q',
                                         'exchange_type': 'direct', 'exchange_name': 'wotnot.direct',
                                         'worker_count': 0}


def connect():
    parameters = pika.URLParameters(RABBIT_MQ_URL_PARAMETER)
    connection = pika.BlockingConnection(parameters)
    return connection


def close(connection):
    if connection is not None:
        connection.close()


class RabbitMqConnection(object):
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        close(self.conn)


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


class RecoverConversationConsumer(Thread):
    def __init__(self, queue_name, consumer_name):
        self.queue_name = queue_name
        self._connection = None
        self._channel = None
        self.consumer_name=consumer_name
        Thread.__init__(self)
        Thread.daemon = True
        self.start()

    def callback(self, ch, method, properties, body):
        params = body.decode('utf-8')
        try:
            params = json.loads(params)
            RecoverConversationService(params, self.consumer_name).do_process()
        except AMQPError:
            print("AMQPError: Restarting the consumer")
            publish_message(QueueEnum.FAILED_CONVERSATION_DATA_RECOVERY.value['route'],
                            json.loads(params))
            self.run()
        except Exception as e:
            print("Received Exception: {e} while processing the Message: {message}".format(e=e, message=params))
            publish_message(QueueEnum.FAILED_CONVERSATION_DATA_RECOVERY.value['route'],
                            json.loads(params))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        self._connection = connect()
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(on_message_callback=self.callback, queue=self.queue_name, auto_ack=False)
        self._channel.start_consuming()


def start_consumer():
    for i in range(QueueEnum.CONVERSATION_DATA_RECOVERY.value['worker_count']):
        # time.sleep(1)
        RecoverConversationConsumer(queue_name="test1", consumer_name=i)


# Add is_new_visitor service class
class RecoverConversationService:
    def __init__(self, payload, consumer_name):
        self.payload = payload
        self.consumer_name = consumer_name

    def do_process(self):
        print(self.payload['conversation_key'], "-->", self.consumer_name)


app = Flask(__name__)
if __name__ == "__main__":
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
    start_consumer()
    app.run(port=3000)

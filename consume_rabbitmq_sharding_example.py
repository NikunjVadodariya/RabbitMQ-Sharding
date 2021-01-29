import json
import time
from threading import Thread

import pika
from flask import Flask


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


class ShardConsumer(Thread):
    def __init__(self, queue_name, consumer_name):
        self.queue_name = queue_name
        self._connection = None
        self._channel = None
        self.consumer_name = consumer_name
        Thread.__init__(self)
        Thread.daemon = True
        self.start()

    def callback(self, ch, method, properties, body):
        params = body.decode('utf-8')
        params = json.loads(params)
        print(params['conversation_key'], "-->", self.consumer_name)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        self._connection = connect()
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(on_message_callback=self.callback, queue=self.queue_name, auto_ack=False)
        self._channel.start_consuming()


def start_consumer():
    for i in range(2):
        time.sleep(1)
        ShardConsumer(queue_name="sharding_test", consumer_name=i)


app = Flask(__name__)
if __name__ == "__main__":
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

    start_consumer()
    app.run(port=3000)

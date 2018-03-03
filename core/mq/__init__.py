# -*- coding: utf-8 -*-

"""

    Module :mod:``

    This Module is created to...

    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
import json
import uuid
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
import pika
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
from core.utils.environ import (
    get_queue_details,
    get_normalized_rmq_env_details,
    get_normalized_rmq_credentials
)
# ----------- END: In-App Imports ---------- #


__import__('pkg_resources').declare_namespace(__name__)


__all__ = [
    # All public symbols go here.
]


class SimpleRabbitMQ(object):
    def __init__(self, **mq_details):

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                **mq_details
            )
        )

        self.channel = self.connection.channel()

    def _publish(self, queue, exchange, queue_durable=False, payload=None):

        if payload:

            self.channel.queue_declare(queue=queue, durable=queue_durable)

            self.channel.basic_publish(
                exchange=exchange,
                routing_key=queue,
                body=json.dumps(payload)
            )

        self.close_conn()

    def consume(self, queue, callback):
        self.channel.basic_consume(
            callback, queue=queue, no_ack=True
        )

        self.channel.start_consuming()

    def close_conn(self):
        self.connection.close()


class SimpleConsumer(SimpleRabbitMQ):
    def __init__(self, **mq_details):
        super(self.__class__, self).__init__(**mq_details)

    def on_response(self, ch, method, properties, body):
        print(" [x] Received %r" % body)

    def listen(self, queue):
        self.consume(queue, self.on_response)


class SimpleProducer(SimpleRabbitMQ):
    def __init__(self, **mq_details):
        super(self.__class__, self).__init__(**mq_details)

    def publish(self, queue, payload):
        self.publish(queue, payload)


class SimpleCentralizedLogProducer(SimpleRabbitMQ):

    def __init__(self):

        rmq_env_details = get_normalized_rmq_env_details()
        rmq_cred_details = get_normalized_rmq_credentials()

        credentials = pika.credentials.PlainCredentials(**rmq_cred_details)

        super(self.__class__, self).__init__(credentials=credentials, **rmq_env_details)

        self.queue_name, self.queue_durable = get_queue_details()['central_logger_queue']

    def publish(self, payload):
        self._publish(
            queue=self.queue_name,
            exchange='test_exchange',
            queue_durable=self.queue_durable,
            payload=payload
        )

        self.close_conn()


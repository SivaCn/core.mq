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

    def publish(self, queue, payload):

        self.channel.queue_declare(queue=queue)

        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=payload
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

    def emit(self, queue, payload):
        self.publish(queue, payload)

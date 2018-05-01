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
    def __init__(self, enable_auxiliary=False, **mq_details):

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                **mq_details
            )
        )

        self.channel = self.connection.channel()

        self.aux_channel = None

        if enable_auxiliary:
            self.aux_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    **mq_details
                )
            )

            self.aux_channel = self.connection.channel()

    def _publish(self, queue, exchange, queue_durable=False, properties=None, queue_declare=True, payload=None):

        if payload:

            if queue_declare:
                self.channel.queue_declare(queue=queue, durable=queue_durable)

            self.channel.basic_publish(
                exchange=exchange,
                routing_key=queue,
                body=json.dumps(payload),
                properties=properties
            )

    def consume(self, queue, callback):
        self.channel.basic_consume(
            callback, queue=queue, no_ack=True
        )

        self.channel.start_consuming()

    def close_conn(self):
        self.connection.close()

        if self.aux_channel:
            self.aux_connection.close()


class SimpleConsumer(SimpleRabbitMQ):
    def __init__(self, **mq_details):
        super(self.__class__, self).__init__(**mq_details)

    def on_response(self, ch, method, properties, body):
        print(" [x] Received %r" % body)

    def listen(self, queue):
        self.consume(queue, self.on_response)


class SimplePublisher(SimpleRabbitMQ):
    def __init__(self):

        rmq_env_details = get_normalized_rmq_env_details()
        rmq_cred_details = get_normalized_rmq_credentials()

        credentials = pika.credentials.PlainCredentials(**rmq_cred_details)

        super(self.__class__, self).__init__(credentials=credentials, **rmq_env_details)

    def publish(self, queue_name, payload, durable=False):

        self.queue_name, self.queue_durable = queue_name, durable

        self._publish(
            queue_name,
            exchange='test_exchange',
            payload=payload,
            queue_durable=durable,
        )


class SimpleCentralizedLogProducer(SimpleRabbitMQ):

    def __init__(self):

        rmq_env_details = get_normalized_rmq_env_details()
        rmq_cred_details = get_normalized_rmq_credentials()

        credentials = pika.credentials.PlainCredentials(**rmq_cred_details)

        super(self.__class__, self).__init__(credentials=credentials, **rmq_env_details)

        self.queue_name, self.queue_durable = get_queue_details()['central_logger_queue']

    def publish(self, *args, **kwargs):
        self._publish(
            queue=self.queue_name,
            exchange='test_exchange',
            queue_durable=self.queue_durable,
            payload=kwargs
        )

        self.close_conn()


class SimpleSchedulerPublisher(SimpleRabbitMQ):
    pass


class RPCSchedulerPublisher(SimpleRabbitMQ):

    def __init__(self):
        rmq_env_details = get_normalized_rmq_env_details()
        rmq_cred_details = get_normalized_rmq_credentials()

        credentials = pika.credentials.PlainCredentials(**rmq_cred_details)

        super(self.__class__, self).__init__(
            credentials=credentials, enable_auxiliary=True, **rmq_env_details
        )

        self.queue_name, self.queue_durable = get_queue_details()['scheduler_queue']

    def on_response(self, ch, method, props, body):

        self.response = json.loads(body)

        self.aux_channel.stop_consuming()
        self.aux_channel.queue_delete(self.reply_to_queue)

    def on_timeout():

        _message = 'Request timed out'

        self.response = {
            'result': False,
            'message': '',
            'error_message': _message,
            'error_trace': ''
        }

        self.aux_channel.stop_consuming()
        self.aux_channel.queue_delete(self.reply_to_queue)


    def publish(self, *args, **kwargs):

        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.reply_to_queue = 'response-q-{}'.format(str(uuid.uuid4()))

        kwargs['reply_to_queue'] = self.reply_to_queue

        self._publish(
            queue=self.queue_name,
            exchange='test_exchange',
            queue_durable=self.queue_durable,
            payload=kwargs,
        )

        # self.aux_channel.queue_declare(queue=self.reply_to_queue)

        # self.aux_channel.basic_consume(
        #     self.on_response, no_ack=True, queue=self.reply_to_queue
        # )

        #self.aux_connection.add_timeout(10, self.on_timeout)

        # self.aux_channel.start_consuming()

        self.close_conn()

        return self.response

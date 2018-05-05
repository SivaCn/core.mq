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

from core.utils.environ import get_normalized_rmq_credentials
# ----------- END: In-App Imports ---------- #


__import__('pkg_resources').declare_namespace(__name__)


__all__ = [
    # All public symbols go here.
]


queue_details = get_queue_details()


class SimpleRabbitMQ(object):

    def __init__(self):

        rmq_env_details = {
            key: value
            for key, value in
            get_normalized_rmq_env_details().items()
            if key in ('host','port', 'virtual_host', )
        }

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                credentials=pika.credentials.PlainCredentials(
                    **get_normalized_rmq_credentials()
                ),
                **rmq_env_details
            )
        )

        self.channel = self.connection.channel()

    def publish(self, queue=None, exchange=None, queue_durable=False, properties=None, queue_declare=True, payload=None):

        queue = queue or self.queue_name
        queue_durable = queue_durable or self.queue_durable
        exchange = exchange or get_normalized_rmq_env_details()['exchange']

        is_published = False

        if payload:

            if queue_declare:
                self.channel.queue_declare(queue=queue, durable=queue_durable)

            self.channel.confirm_delivery()

            is_published = self.channel.basic_publish(
                routing_key=queue,
                exchange=exchange,
                body=json.dumps(payload),
                properties=properties or pika.BasicProperties(delivery_mode=1)
            )

        self.connection.close()

        return is_published


class SimpleCentralLogPublisher(SimpleRabbitMQ):

    def __init__(self):

        super(self.__class__, self).__init__()

        self.queue_name, self.queue_durable = queue_details['central_logger_queue']


class SimpleSMSPublisher(SimpleRabbitMQ):

    def __init__(self):

        super(self.__class__, self).__init__()

        self.queue_name, self.queue_durable = queue_details['central_sms_queue']


class SimpleSchedulerPublisher(SimpleRabbitMQ):

    def __init__(self):

        super(self.__class__, self).__init__()

        self.queue_name, self.queue_durable = queue_details['scheduler_queue']


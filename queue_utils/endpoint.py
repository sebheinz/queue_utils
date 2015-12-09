import logging
import pika
import json


# TODO: See the example at:
# http://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html
# This queue implementation could be expanded.
class RabbitMQQueue():
    def __init__(self, url, exchange, id, get_routing_key=None):
        self._params = pika.URLParameters(url)
        self._params.socket_timeout = 5

        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()

        # Declare the exchange.
        self._exchange = exchange
        self._channel.exchange_declare(exchange=exchange, type='direct')

        self._id = id

        # Set the quality of service properties.
        self._channel.basic_qos(prefetch_count=1)

        # Store some send properties.
        self._send_properties = pika.BasicProperties(delivery_mode=2)

        if get_routing_key is None:
            self.get_routing_key = self._default_routing_key
        else:
            self.get_routing_key = get_routing_key

    def _create_queue_for_routing_key(self, routing_key):
        logging.info("Creating queue for %s" % routing_key)
        # Declare the required queue
        self._channel.queue_declare(queue=routing_key, durable=True)
        # Bind the queue to the exchange.
        self._channel.queue_bind(exchange=self._exchange,
                                 queue=routing_key,
                                 routing_key=routing_key)

    def _default_routing_key(self, payload):
        return self._id

    def listen(self, func):
        """
        Listen on the endpoint, calling the specified function if something is
        received.
        """

        def json_func(ch, method, properties, payload):
            func(ch, method, properties, json.loads(payload))

        self._create_queue_for_routing_key(self._id)

        logging.info("Start listening on queue %s" % self._id)
        # Install the channel handler.
        self._channel.basic_consume(json_func, queue=self._id)
        # Start listening on the channel.
        self._channel.start_consuming()

    def send(self, payload):
        """
        Send the payload on the queue.
        """
        routing_key = self.get_routing_key(payload)
        self._create_queue_for_routing_key(routing_key)

        logging.info("Sending to %s" % routing_key)

        result = self._channel.basic_publish(exchange=self._exchange,
                                             routing_key=routing_key,
                                             body=json.dumps(payload),
                                             properties=self._send_properties)

        logging.info("result=%s" % result)

    def ack(self, channel, method, is_nack):
        """
        Acknowlege the receipt of a message.
        """
        if not is_nack:
            logging.info("nack is not implemented")

        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def close(self):
        """
        Close the connection
        """
        self._connection.close()


class InputOutputEndpoint(object):
    """
    A basic enpoint that can be used for testing.
    """
    def __init__(self):
        self._func = None
        self._results = []
        self._acks = []

    def listen(self, func):
        self._func = func

    def send(self, payload):
        self._results.append(payload)

    def ack(self, channel, method, is_nack):
        self._acks.append(not is_nack)

    # Utility functions for testing.
    def push(self, payload):
        self._func(None, None, None, payload)

    def pull(self):
        return self._results

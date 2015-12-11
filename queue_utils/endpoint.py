import logging
import pika
import json


# TODO: See the example at:
# http://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html
# This queue implementation could be expanded.
class RabbitMQQueue():
    def __init__(self,
                 url,
                 exchange,
                 input_id,
                 output_id,
                 get_output_routing_key=None):
        self._params = pika.URLParameters(url)
        self._params.socket_timeout = 5

        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()

        # Declare the exchange.
        self._exchange = exchange
        self._channel.exchange_declare(exchange=exchange, type='direct')

        self._input_id = input_id
        self._output_id = output_id

        # Set the quality of service properties.
        self._channel.basic_qos(prefetch_count=1)

        # Store some send properties.
        self._send_properties = pika.BasicProperties(delivery_mode=2)

        if get_output_routing_key is None:
            self.get_output_routing_key = self._default_output_routing_key
        else:
            self.get_output_routing_key = get_output_routing_key

    def __str__(self):
        return str((self._input_id, self._output_id))

    def _create_queue_for_routing_key(self, routing_key):
        logging.info("Creating queue for %s" % routing_key)

        # Declare the required queue
        self._channel.queue_declare(queue=routing_key, durable=True)
        # Bind the queue to the exchange.
        self._channel.queue_bind(exchange=self._exchange,
                                 queue=routing_key,
                                 routing_key=routing_key)

    def _default_output_routing_key(self, routing_key, payload):
        return routing_key

    def listen_on_input(self, func):
        """
        Listen on the endpoint, calling the specified function if something is
        received.
        """

        def json_func(ch, method, properties, payload):
            func(ch, method, properties, json.loads(payload))

        self._create_queue_for_routing_key(self._input_id)

        logging.info("Start listening on queue %s" % self._input_id)
        # Install the channel handler.
        self._channel.basic_consume(json_func, queue=self._input_id)
        # Start listening on the channel.
        self._channel.start_consuming()

    def send_to_output(self, payload):
        """
        Send the payload on the queue.
        """
        r_key = self.get_output_routing_key(self._output_id, payload)
        self._create_queue_for_routing_key(r_key)

        logging.info("Sending to %s" % r_key)

        result = self._channel.basic_publish(exchange=self._exchange,
                                             routing_key=r_key,
                                             body=json.dumps(payload),
                                             properties=self._send_properties)

        logging.info("result=%s" % result)

    def acknowlege_input(self, delivery_tag, is_nack):
        """
        Acknowlege the receipt of a message.
        """
        if is_nack:
            logging.info("nack is not implemented")

        self._channel.basic_ack(delivery_tag=delivery_tag)

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

    def listen_on_input(self, func):
        self._func = func

    def send_to_output(self, payload):
        self._results.append(payload)

    def acknowlege_input(self, delivery_tag, is_nack):
        self._acks.append(not is_nack)

    # Utility functions for testing.
    def push(self, payload):
        self._func(None, None, None, payload)

    def pull(self):
        return self._results

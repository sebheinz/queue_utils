import logging


class Worker(object):

    """
    A Worker is connected to a bidirectional channel, listening on the input
    port, processing the message recieved, and sending the output to the output
    port. This allows a Worker to act as both a consumer and a producer when
    used with queueing paradigms.
    """

    def __init__(self, channel, work_method, payload_check=None):
        """
        channel: A bi-directional connection with a defined input and output.
        work_method: A hook which is used to process the message payload
                     after some initial checks have been performed.
        payload_check: A hook that can be used to check the validity of a
                       message payload.
        """
        logging.info("Creating worker with channel %s" % channel)
        self._channel = channel
        self._work_method = work_method
        self._payload_check = payload_check

    def start(self):
        """
        Start the worker as a consumer of the input port. The method get_work
        is registered as a callback for processing incomming messages.
        """
        logging.info("Starting worker")
        self._channel.listen_on_input(self.get_work)

    def get_work(self, ch, method, properties, payload):
        """
        A callback wich is used for processing incomming messages. Basic error
        checks are performed, before calling the work hook registered at
        construction.
        """
        try:
            payload_text = payload.keys()
        except:
            payload_text = payload
        logging.info("Processing work unit with payload keys: %s" %
                     payload_text)
        # TODO: Add logging as required. e.g.
        # print " [x] Received ", payload.keys()
        # if JOB in payload.keys():
        #     print "job:", payload[JOB]
        # if ID in payload.keys():
        #     print "job id:", payload[ID]
        # if COLLECTION in payload.keys():
        #     print "collection:", payload[COLLECTION]
        # logging.info("START working on job_id: %s, collection: %s",
        # payload[ID], payload[COLLECTION])

        # Simply forward an existing error.
        if "error" in payload:
            if payload["error"]:
                logging.error("Forwarding error: %s" % payload["error"])
                self.send(payload)
                self.acknowledge(method)
                return

        # Check if the payload is valid.
        is_valid, error = self.is_valid_payload(payload)
        if not is_valid:
            self.send(error)
            self.acknowledge(method, True)
            return

        # Get the results of the worker.
        results, error = self.do_work(payload)

        # Send the results.
        self.send(results)

        # Only acknowledge the receipt of the message if there were no errors.
        if not error:
            self.acknowledge(method)

        logging.info("Done processing work unit")

    def is_valid_payload(self, payload):
        """
        Check if the specified payload is valid. All payloads are considered
        valid by defaults, but a validity check can be included during
        construction.
        """
        # Checking if the payload is valid.
        logging.info("Checking for valid payload")

        is_valid = True
        error = None

        if self._payload_check is None:
            return is_valid, error

        return self._payload_check(payload)

    def do_work(self, payload):
        """
        Call the work hook specified at construction.
        """
        logging.info("Doing work")

        results, error = self._work_method(payload)

        return results, error

    def send(self, payload):
        """
        Send the payload to the output port of the channel with which the
        worker is associated.
        """
        logging.info("Sending payload")

        self._channel.send_to_output(payload)

    def acknowledge(self, method, is_nack=False):
        """
        Acknowledge the processing on the input.
        """
        logging.info("Sending acknowledgement (is_nack=%s)" % is_nack)

        if method is None:
            delivery_tag = None
        else:
            delivery_tag = method.delivery_tag
        self._channel.acknowlege_input(delivery_tag, is_nack)

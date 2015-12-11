import logging


class Worker(object):
    def __init__(self, channel, work_method, payload_check=None):
        logging.info("Creating worker with channel %s" % channel)
        self._channel = channel
        self._work_method = work_method
        self._payload_check = payload_check

    def start(self):
        logging.info("Starting worker")
        self._channel.listen_on_input(self.get_work)

    def get_work(self, ch, method, properties, payload):
        # Perform logging.
        logging.info("Processing work unit with payload: %s" % payload)
        # TODO: Add logging as required. e.g.
        # collection_content = json.loads(body)
        # print " [x] Received ", collection_content.keys()
        # if JOB in collection_content.keys():
        #     print "job:", collection_content[JOB]
        # if ID in collection_content.keys():
        #     print "job id:", collection_content[ID]
        # if COLLECTION in collection_content.keys():
        #     print "collection:", collection_content[COLLECTION]
        # logging.info("START working on job_id: %s, collection: %s",
        # collection_content[ID], collection_content[COLLECTION])

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
        # Checking if the payload is valid.
        logging.info("Checking for valid payload")

        is_valid = True
        error = None

        if self._payload_check is None:
            return is_valid, error

        return self._payload_check(payload)

    def do_work(self, payload):
        """
        Do the work specified by the payload.
        """
        logging.info("Doing work")

        results, error = self._work_method(payload)

        return results, error

    def send(self, payload):
        """
        Send the specified payload.
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

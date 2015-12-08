import os
import sys
import logging

logging.basicConfig(level=logging.DEBUG)

root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
sys.path.insert(0, os.path.join(root, 'src'))

from queue_utils import Worker


class InputOutput():
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


def identity_work_method(payload):
    return payload, None


def foo_work_method(payload):
    return "FOO", None


invalid_payload_error = "INVALID_PAYLOAD"


def all_payloads_are_invalid(payload):
    return False, invalid_payload_error


def test_basic_worker():
    input = InputOutput()
    output = InputOutput()
    w = Worker(input, output, identity_work_method)
    w.start()

    input.push("TEST")

    assert output.pull() == ["TEST", ]
    assert input._acks == [True, ]


def test_invalid_payload_sends_error_and_nacks():

    input = InputOutput()
    output = InputOutput()
    w = Worker(input, output, identity_work_method, all_payloads_are_invalid)
    w.start()

    input.push("TEST")

    assert output.pull() == [invalid_payload_error, ]
    assert input._acks == [False, ]


def test_forward_payload_error():
    input = InputOutput()
    output = InputOutput()

    w = Worker(input, output, foo_work_method)
    w.start()

    error_payload = {"error": "Some error", }
    input.push(error_payload)

    assert output.pull() == [error_payload, ]
    assert input._acks == [True, ]


def test_error_field_with_no_error():
    input = InputOutput()
    output = InputOutput()

    w = Worker(input, output, foo_work_method)
    w.start()

    error_payload = {"error": {}, }
    input.push(error_payload)

    assert output.pull() == ["FOO", ]
    assert input._acks == [True, ]

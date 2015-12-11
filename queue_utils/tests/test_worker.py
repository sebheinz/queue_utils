from queue_utils import Worker
from queue_utils import InputOutputEndpoint


def identity_work_method(payload):
    return payload, None


def work_method(payload):
    return "WORKED", None


invalid_payload_error = "INVALID_PAYLOAD"


def all_payloads_are_invalid(payload):
    return False, invalid_payload_error


def test_basic_worker():
    channel = InputOutputEndpoint()
    w = Worker(channel, identity_work_method)
    w.start()

    channel.push("TEST")

    assert channel.pull() == ["TEST", ]
    assert channel._acks == [True, ]


def test_invalid_payload_sends_error_and_nacks():

    channel = InputOutputEndpoint()
    w = Worker(channel, identity_work_method, all_payloads_are_invalid)
    w.start()

    channel.push("TEST")

    assert channel.pull() == [invalid_payload_error, ]
    assert channel._acks == [False, ]


def test_forward_payload_error():
    channel = InputOutputEndpoint()
    w = Worker(channel, work_method)
    w.start()

    error_payload = {"error": "Some error", }
    channel.push(error_payload)

    assert channel.pull() == [error_payload, ]
    assert channel._acks == [True, ]


def test_error_field_with_no_error():
    channel = InputOutputEndpoint()
    w = Worker(channel, work_method)
    w.start()

    error_payload = {"error": {}, }
    channel.push(error_payload)

    assert channel.pull() == ["WORKED", ]
    assert channel._acks == [True, ]

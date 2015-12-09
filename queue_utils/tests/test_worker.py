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
    input = InputOutputEndpoint()
    output = InputOutputEndpoint()
    w = Worker(input, output, identity_work_method)
    w.start()

    input.push("TEST")

    assert output.pull() == ["TEST", ]
    assert input._acks == [True, ]


def test_invalid_payload_sends_error_and_nacks():

    input = InputOutputEndpoint()
    output = InputOutputEndpoint()
    w = Worker(input, output, identity_work_method, all_payloads_are_invalid)
    w.start()

    input.push("TEST")

    assert output.pull() == [invalid_payload_error, ]
    assert input._acks == [False, ]


def test_forward_payload_error():
    input = InputOutputEndpoint()
    output = InputOutputEndpoint()

    w = Worker(input, output, work_method)
    w.start()

    error_payload = {"error": "Some error", }
    input.push(error_payload)

    assert output.pull() == [error_payload, ]
    assert input._acks == [True, ]


def test_error_field_with_no_error():
    input = InputOutputEndpoint()
    output = InputOutputEndpoint()

    w = Worker(input, output, work_method)
    w.start()

    error_payload = {"error": {}, }
    input.push(error_payload)

    assert output.pull() == ["WORKED", ]
    assert input._acks == [True, ]

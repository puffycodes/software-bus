"""The default limits and timings are the values the specs in docs/design/ give.

Tests shrink these per instance or monkeypatch them, so a wrong default would
otherwise go unnoticed.
"""
import pytest

from software_bus import _cli, request_reply
from software_bus.base_layer import MAX_PAYLOAD_LENGTH, BaseLayerNode
from software_bus.client import BaseLayerClient
from software_bus.pubsub import _MAX_SUBJECT_LENGTH, PubSubNode

MIB = 1024 * 1024


@pytest.mark.parametrize(
    "actual, expected",
    [
        # base-layer.md, Definitions
        (MAX_PAYLOAD_LENGTH, 16 * MIB),
        (BaseLayerNode.send_timeout, 30),
        (BaseLayerClient.send_timeout, 30),
        (BaseLayerClient.close_timeout, 5),
        (BaseLayerNode.send_queue_limit, 64 * MIB),
        (_cli.FIRST_RECONNECT_DELAY, 1),
        (_cli.MAX_RECONNECT_DELAY, 60),
        # publish-subscribe.md, Definitions and Cycle Prevention
        (PubSubNode.check_timeout, 5),
        (PubSubNode.held_messages_limit, 64 * MIB),
        (PubSubNode.max_check_attempts, 5),
        (PubSubNode.check_retry_delay, (0.2, 2.0)),
        # data-format.md: the 2-byte subject length
        (_MAX_SUBJECT_LENGTH, 65535),
        # request-reply.md
        (request_reply.DEFAULT_TIMEOUT, 300),
        (request_reply.UNIQUE_ID_LENGTH, 32),
    ],
    ids=[
        "maximum payload length",
        "node send timeout",
        "client send timeout",
        "client close timeout",
        "send queue limit",
        "first reconnect delay",
        "maximum reconnect delay",
        "check timeout",
        "held messages limit",
        "check attempts",
        "check retry delay",
        "maximum subject length",
        "request timeout",
        "unique id length",
    ],
)
def test_default_matches_the_spec(actual, expected):
    assert actual == expected

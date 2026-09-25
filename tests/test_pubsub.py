import asyncio
import logging

import pytest

from software_bus import PubSubClient, PubSubNode
from software_bus.pubsub import PublishMessage, SubscriptionMessage, decode_message, encode_message

from helpers import accept_one_peer_connection


def test_encode_decode_subscription_roundtrip():
    for subscribe in (True, False):
        message = SubscriptionMessage(subject="a.b.c", subscribe=subscribe)
        decoded = decode_message(encode_message(message))
        assert decoded == message


def test_encode_decode_publish_roundtrip_with_binary_payload():
    payload = bytes(range(256))
    message = PublishMessage(subject="a.b", payload=payload)
    decoded = decode_message(encode_message(message))
    assert decoded == message


@pytest.mark.parametrize(
    "message",
    [
        SubscriptionMessage(subject="", subscribe=True),
        SubscriptionMessage(subject="données.é", subscribe=False),
        PublishMessage(subject="", payload=b""),
        PublishMessage(subject="données.é", payload=b""),
    ],
)
def test_encode_decode_roundtrip_edge_cases(message):
    assert decode_message(encode_message(message)) == message


def test_decode_unknown_message_type_raises():
    with pytest.raises(ValueError):
        decode_message(b"\xff")


@pytest.mark.asyncio
async def test_subscribe_sends_wire_message_only_for_first_callback_on_a_subject():
    server, connected = await accept_one_peer_connection()
    port = server.sockets[0].getsockname()[1]
    client = PubSubClient()
    try:
        await client.connect("127.0.0.1", port)
        peer = await asyncio.wait_for(connected, timeout=1)

        await client.subscribe("a.b", lambda matched, actual, payload: None)
        await client.subscribe("a.b", lambda matched, actual, payload: None)

        first = await asyncio.wait_for(peer.receive(), timeout=1)
        assert decode_message(first) == SubscriptionMessage("a.b", subscribe=True)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer.receive(), timeout=0.1)
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unsubscribe_sends_wire_message_only_when_last_callback_removed():
    server, connected = await accept_one_peer_connection()
    port = server.sockets[0].getsockname()[1]
    client = PubSubClient()

    def callback_a(matched, actual, payload):
        pass

    def callback_b(matched, actual, payload):
        pass

    try:
        await client.connect("127.0.0.1", port)
        peer = await asyncio.wait_for(connected, timeout=1)

        await client.subscribe("a.b", callback_a)
        await client.subscribe("a.b", callback_b)
        await asyncio.wait_for(peer.receive(), timeout=1)  # the single subscribe message

        await client.unsubscribe("a.b", callback_a)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer.receive(), timeout=0.1)

        await client.unsubscribe("a.b", callback_b)
        last = await asyncio.wait_for(peer.receive(), timeout=1)
        assert decode_message(last) == SubscriptionMessage("a.b", subscribe=False)
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_publish_delivered_only_to_subscribed_client():
    node = PubSubNode()
    subscriber = PubSubClient()
    other = PubSubClient()
    received = []
    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        await subscriber.connect("127.0.0.1", port)
        await other.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        await subscriber.subscribe(
            "a.b", lambda matched, actual, payload: received.append((matched, actual, payload))
        )
        await asyncio.sleep(0.05)

        publisher = PubSubClient()
        await publisher.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        try:
            await publisher.publish("a.b", b"hello")
            await asyncio.sleep(0.1)
        finally:
            await publisher.close()

        assert received == [("a.b", "a.b", b"hello")]
    finally:
        await subscriber.close()
        await other.close()
        await node.close()


@pytest.mark.asyncio
async def test_publish_not_delivered_for_unrelated_subject():
    node = PubSubNode()
    subscriber = PubSubClient()
    received = []
    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        await subscriber.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe(
            "a.b", lambda matched, actual, payload: received.append((matched, actual, payload))
        )
        await asyncio.sleep(0.05)

        publisher = PubSubClient()
        await publisher.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        try:
            await publisher.publish("x.y", b"nope")
            await asyncio.sleep(0.1)
        finally:
            await publisher.close()

        assert received == []
    finally:
        await subscriber.close()
        await node.close()


@pytest.mark.asyncio
async def test_subscription_propagates_upstream_and_publish_flows_down_the_tree():
    root = PubSubNode()
    mid = PubSubNode()
    subscriber = PubSubClient()
    publisher = PubSubClient()
    received = []
    try:
        root_server = await root.accept_connection(port=0)
        root_port = root_server.sockets[0].getsockname()[1]

        mid_server = await mid.accept_connection(port=0)
        mid_port = mid_server.sockets[0].getsockname()[1]
        await mid.establish_connection("127.0.0.1", root_port)
        await asyncio.sleep(0.05)

        await subscriber.connect("127.0.0.1", mid_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe(
            "a.b", lambda matched, actual, payload: received.append((matched, actual, payload))
        )
        await asyncio.sleep(0.1)

        assert list(mid._downstream_subscriptions.keys()) == ["a.b"]
        assert list(root._downstream_subscriptions.keys()) == ["a.b"]

        await publisher.connect("127.0.0.1", root_port)
        await asyncio.sleep(0.05)
        await publisher.publish("a.b", b"from-root")
        await asyncio.sleep(0.1)

        assert received == [("a.b", "a.b", b"from-root")]
    finally:
        await subscriber.close()
        await publisher.close()
        await mid.close()
        await root.close()


@pytest.mark.asyncio
async def test_unsubscribe_stops_further_delivery_and_propagates_upstream():
    root = PubSubNode()
    mid = PubSubNode()
    subscriber = PubSubClient()
    publisher = PubSubClient()
    received = []
    try:
        root_server = await root.accept_connection(port=0)
        root_port = root_server.sockets[0].getsockname()[1]

        mid_server = await mid.accept_connection(port=0)
        mid_port = mid_server.sockets[0].getsockname()[1]
        await mid.establish_connection("127.0.0.1", root_port)
        await asyncio.sleep(0.05)

        def on_publish(matched, actual, payload):
            received.append((matched, actual, payload))

        await subscriber.connect("127.0.0.1", mid_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b", on_publish)
        await asyncio.sleep(0.1)

        await subscriber.unsubscribe("a.b", on_publish)
        await asyncio.sleep(0.1)

        assert "a.b" not in mid._downstream_subscriptions
        assert "a.b" not in root._downstream_subscriptions

        await publisher.connect("127.0.0.1", root_port)
        await asyncio.sleep(0.05)
        await publisher.publish("a.b", b"should-not-arrive")
        await asyncio.sleep(0.1)

        assert received == []
    finally:
        await subscriber.close()
        await publisher.close()
        await mid.close()
        await root.close()


@pytest.mark.asyncio
async def test_unsubscribe_does_not_propagate_while_other_subscribers_remain():
    node = PubSubNode()
    subscriber_a = PubSubClient()
    subscriber_b = PubSubClient()
    received_b = []
    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        await subscriber_a.connect("127.0.0.1", port)
        await subscriber_b.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        def on_publish_a(matched, actual, payload):
            pass

        await subscriber_a.subscribe("a.b", on_publish_a)
        await subscriber_b.subscribe(
            "a.b", lambda matched, actual, payload: received_b.append((matched, actual, payload))
        )
        await asyncio.sleep(0.1)

        await subscriber_a.unsubscribe("a.b", on_publish_a)
        await asyncio.sleep(0.1)

        # subject still has a subscriber (subscriber_b), so it must remain tracked
        assert "a.b" in node._downstream_subscriptions
        assert len(node._downstream_subscriptions["a.b"]) == 1

        publisher = PubSubClient()
        await publisher.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        try:
            await publisher.publish("a.b", b"still-subscribed")
            await asyncio.sleep(0.1)
        finally:
            await publisher.close()

        assert received_b == [("a.b", "a.b", b"still-subscribed")]
    finally:
        await subscriber_a.close()
        await subscriber_b.close()
        await node.close()


@pytest.mark.asyncio
async def test_subscribe_message_flooded_to_sibling_downstream_client_is_logged(caplog):
    node = PubSubNode()
    first = PubSubClient()
    second = PubSubClient()
    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        await first.connect("127.0.0.1", port)
        await second.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        with caplog.at_level(logging.INFO, logger="software_bus.pubsub"):
            await second.subscribe("a.b", lambda matched, actual, payload: None)
            await asyncio.sleep(0.1)

        assert any(
            "a.b" in record.getMessage() and "subscribe=True" in record.getMessage()
            for record in caplog.records
        )
    finally:
        await first.close()
        await second.close()
        await node.close()


@pytest.mark.asyncio
async def test_downstream_disconnect_removes_subscription_and_propagates_unsubscribe():
    root = PubSubNode()
    mid = PubSubNode()
    subscriber = PubSubClient()
    try:
        root_server = await root.accept_connection(port=0)
        root_port = root_server.sockets[0].getsockname()[1]

        mid_server = await mid.accept_connection(port=0)
        mid_port = mid_server.sockets[0].getsockname()[1]
        await mid.establish_connection("127.0.0.1", root_port)
        await asyncio.sleep(0.05)

        await subscriber.connect("127.0.0.1", mid_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b", lambda matched, actual, payload: None)
        await asyncio.sleep(0.1)
        assert list(root._downstream_subscriptions.keys()) == ["a.b"]

        await subscriber.close()
        await asyncio.sleep(0.1)

        assert mid.downstream_connections == []
        assert mid._downstream_subscriptions == {}
        assert root._downstream_subscriptions == {}
    finally:
        await subscriber.close()
        await mid.close()
        await root.close()


@pytest.mark.asyncio
async def test_upstream_disconnect_removes_subscription_and_propagates_unsubscribe(caplog):
    root = PubSubNode()
    mid = PubSubNode()
    root_subscriber = PubSubClient()
    mid_client = PubSubClient()
    try:
        root_server = await root.accept_connection(port=0)
        root_port = root_server.sockets[0].getsockname()[1]

        mid_server = await mid.accept_connection(port=0)
        mid_port = mid_server.sockets[0].getsockname()[1]
        await mid.establish_connection("127.0.0.1", root_port)
        await asyncio.sleep(0.05)

        await mid_client.connect("127.0.0.1", mid_port)
        await root_subscriber.connect("127.0.0.1", root_port)
        await asyncio.sleep(0.05)

        with caplog.at_level(logging.INFO, logger="software_bus.pubsub"):
            await root_subscriber.subscribe("a.b", lambda matched, actual, payload: None)
            await asyncio.sleep(0.1)
            assert list(mid._upstream_subscriptions.keys()) == ["a.b"]

            await root.close()
            await asyncio.sleep(0.1)

        assert mid.upstream_connections == []
        assert mid._upstream_subscriptions == {}
        messages = [record.getMessage() for record in caplog.records]
        assert any("a.b" in m and "subscribe=True" in m for m in messages)
        assert any("a.b" in m and "subscribe=False" in m for m in messages)
    finally:
        await root_subscriber.close()
        await mid_client.close()
        await mid.close()
        await root.close()

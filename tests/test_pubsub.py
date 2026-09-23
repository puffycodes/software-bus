import asyncio

import pytest

from software_bus import PubSubClient, PubSubNode
from software_bus.pubsub import PublishMessage, SubscriptionMessage, decode_message, encode_message


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


def test_decode_unknown_message_type_raises():
    with pytest.raises(ValueError):
        decode_message(b"\xff")


@pytest.mark.asyncio
async def test_publish_delivered_only_to_subscribed_client():
    node = PubSubNode()
    subscriber = PubSubClient()
    other = PubSubClient()
    received = []
    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        subscriber.register_publish_callback(lambda subject, payload: received.append((subject, payload)))
        other.register_publish_callback(lambda subject, payload: received.append(("other", subject, payload)))

        await subscriber.connect("127.0.0.1", port)
        await other.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        await subscriber.subscribe("a.b")
        await asyncio.sleep(0.05)

        publisher = PubSubClient()
        await publisher.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        try:
            await publisher.publish("a.b", b"hello")
            await asyncio.sleep(0.1)
        finally:
            await publisher.close()

        assert received == [("a.b", b"hello")]
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

        subscriber.register_publish_callback(lambda subject, payload: received.append((subject, payload)))
        await subscriber.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b")
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

        subscriber.register_publish_callback(lambda subject, payload: received.append((subject, payload)))
        await subscriber.connect("127.0.0.1", mid_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b")
        await asyncio.sleep(0.1)

        assert list(mid._downstream_subscriptions.keys()) == ["a.b"]
        assert list(root._downstream_subscriptions.keys()) == ["a.b"]

        await publisher.connect("127.0.0.1", root_port)
        await asyncio.sleep(0.05)
        await publisher.publish("a.b", b"from-root")
        await asyncio.sleep(0.1)

        assert received == [("a.b", b"from-root")]
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

        subscriber.register_publish_callback(lambda subject, payload: received.append((subject, payload)))
        await subscriber.connect("127.0.0.1", mid_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b")
        await asyncio.sleep(0.1)

        await subscriber.subscribe("a.b", subscribe=False)
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

        subscriber_b.register_publish_callback(lambda subject, payload: received_b.append((subject, payload)))

        await subscriber_a.connect("127.0.0.1", port)
        await subscriber_b.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        await subscriber_a.subscribe("a.b")
        await subscriber_b.subscribe("a.b")
        await asyncio.sleep(0.1)

        await subscriber_a.subscribe("a.b", subscribe=False)
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

        assert received_b == [("a.b", b"still-subscribed")]
    finally:
        await subscriber_a.close()
        await subscriber_b.close()
        await node.close()


@pytest.mark.asyncio
async def test_subscribe_message_flooded_to_sibling_downstream_clients():
    node = PubSubNode()
    first = PubSubClient()
    second = PubSubClient()
    subscribe_events = []
    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        first.register_subscribe_callback(lambda subject, state: subscribe_events.append((subject, state)))

        await first.connect("127.0.0.1", port)
        await second.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        await second.subscribe("a.b")
        await asyncio.sleep(0.1)

        assert subscribe_events == [("a.b", True)]
    finally:
        await first.close()
        await second.close()
        await node.close()


@pytest.mark.asyncio
async def test_subscribe_callback_supports_async_callback():
    node = PubSubNode()
    first = PubSubClient()
    second = PubSubClient()
    subscribe_events = []

    async def async_callback(subject, state):
        subscribe_events.append((subject, state))

    try:
        server = await node.accept_connection(port=0)
        port = server.sockets[0].getsockname()[1]

        first.register_subscribe_callback(async_callback)

        await first.connect("127.0.0.1", port)
        await second.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        await second.subscribe("a.b")
        await asyncio.sleep(0.1)

        assert subscribe_events == [("a.b", True)]
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
        await subscriber.subscribe("a.b")
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
async def test_upstream_disconnect_removes_subscription_and_propagates_unsubscribe():
    root = PubSubNode()
    mid = PubSubNode()
    root_subscriber = PubSubClient()
    mid_client = PubSubClient()
    subscriptions = []
    try:
        root_server = await root.accept_connection(port=0)
        root_port = root_server.sockets[0].getsockname()[1]

        mid_server = await mid.accept_connection(port=0)
        mid_port = mid_server.sockets[0].getsockname()[1]
        await mid.establish_connection("127.0.0.1", root_port)
        await asyncio.sleep(0.05)

        mid_client.register_subscribe_callback(
            lambda subject, subscribe: subscriptions.append((subject, subscribe))
        )
        await mid_client.connect("127.0.0.1", mid_port)
        await root_subscriber.connect("127.0.0.1", root_port)
        await asyncio.sleep(0.05)
        await root_subscriber.subscribe("a.b")
        await asyncio.sleep(0.1)
        assert list(mid._upstream_subscriptions.keys()) == ["a.b"]

        await root.close()
        await asyncio.sleep(0.1)

        assert mid.upstream_connections == []
        assert mid._upstream_subscriptions == {}
        assert subscriptions == [("a.b", True), ("a.b", False)]
    finally:
        await root_subscriber.close()
        await mid_client.close()
        await mid.close()
        await root.close()

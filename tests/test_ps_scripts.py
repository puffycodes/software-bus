import argparse
import asyncio

import pytest

from software_bus import ps_publish, ps_server, ps_subscribe
from software_bus._cli import parse_subject_list
from software_bus.base_layer import DEFAULT_HOST, DEFAULT_PORT
from software_bus.pubsub import PubSubClient, PubSubNode, SubscriptionMessage, decode_message, encode_message

from helpers import accept_one_peer_connection, free_port, open_peer_connection


def test_parse_subject_list_splits_and_strips():
    assert parse_subject_list("a.b, c.d ,e.f") == ["a.b", "c.d", "e.f"]


def test_parse_subject_list_rejects_empty_input():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_subject_list("  ,  ,")


def test_ps_server_arg_parser_defaults_to_no_addresses():
    args = ps_server.build_arg_parser().parse_args([])
    assert args.upstream == []
    assert args.listen == []


def test_ps_server_arg_parser_accumulates_repeated_flags():
    args = ps_server.build_arg_parser().parse_args(
        ["--upstream", "10.0.0.1:1111", "--listen", "0.0.0.0:2222"]
    )
    assert args.upstream == [("10.0.0.1", 1111)]
    assert args.listen == [("0.0.0.0", 2222)]


def test_ps_subscribe_arg_parser_defaults():
    args = ps_subscribe.build_arg_parser().parse_args([])
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)
    assert args.subject == []
    assert args.time_stamp is True


def test_ps_subscribe_arg_parser_parses_comma_separated_subjects():
    args = ps_subscribe.build_arg_parser().parse_args(["--subject", "a.b, c.d ,e.f"])
    assert args.subject == ["a.b", "c.d", "e.f"]


def test_ps_publish_arg_parser_defaults():
    args = ps_publish.build_arg_parser().parse_args([])
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)
    assert args.subject is None
    assert args.message is None
    assert args.repeat_count == 1
    assert args.repeat_interval == 1.0


def test_ps_publish_arg_parser_custom_options():
    args = ps_publish.build_arg_parser().parse_args(
        [
            "--subject",
            "a.b",
            "--message",
            "hi",
            "--repeat-count",
            "3",
            "--repeat-interval",
            "0.5",
        ]
    )
    assert args.subject == "a.b"
    assert args.message == "hi"
    assert args.repeat_count == 3
    assert args.repeat_interval == 0.5


@pytest.mark.asyncio
async def test_ps_server_run_listens_and_relays_subscription_upstream():
    upstream_server, upstream_connected = await accept_one_peer_connection()
    upstream_port = upstream_server.sockets[0].getsockname()[1]
    listen_port = free_port()

    task = asyncio.ensure_future(
        ps_server.run([("127.0.0.1", upstream_port)], [("127.0.0.1", listen_port)])
    )
    downstream_connection = None
    try:
        upstream_peer = await asyncio.wait_for(upstream_connected, timeout=1)
        await asyncio.sleep(0.05)  # let the server task reach accept_connection

        downstream_connection = await open_peer_connection("127.0.0.1", listen_port)
        await asyncio.sleep(0.05)

        await downstream_connection.send(encode_message(SubscriptionMessage("a.b", True)))
        received = await asyncio.wait_for(upstream_peer.receive(), timeout=1)
        assert decode_message(received) == SubscriptionMessage("a.b", True)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if downstream_connection is not None:
            await downstream_connection.close()
        upstream_server.close()
        await upstream_server.wait_closed()


@pytest.mark.asyncio
async def test_ps_subscribe_run_prints_received_publish(capsys):
    hub = PubSubNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    task = asyncio.ensure_future(ps_subscribe.run("127.0.0.1", hub_port, ["a.b"]))
    publisher = PubSubClient()
    try:
        await asyncio.sleep(0.1)  # let ps_subscribe connect and subscribe

        await publisher.connect("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)
        await publisher.publish("a.b", b"hello")
        await asyncio.sleep(0.1)

        captured = capsys.readouterr()
        assert "a.b" in captured.out
        assert "hello" in captured.out
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await publisher.close()
        await hub.close()


@pytest.mark.asyncio
async def test_ps_subscribe_run_without_time_stamp_omits_timestamp(capsys):
    hub = PubSubNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    task = asyncio.ensure_future(
        ps_subscribe.run("127.0.0.1", hub_port, ["a.b"], time_stamp=False)
    )
    publisher = PubSubClient()
    try:
        await asyncio.sleep(0.1)

        await publisher.connect("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)
        await publisher.publish("a.b", b"hello")
        await asyncio.sleep(0.1)

        captured = capsys.readouterr()
        assert captured.out.strip() == "a.b: hello"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await publisher.close()
        await hub.close()


@pytest.mark.asyncio
async def test_ps_subscribe_run_without_subjects_does_not_subscribe():
    hub = PubSubNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    task = asyncio.ensure_future(ps_subscribe.run("127.0.0.1", hub_port, []))
    try:
        await asyncio.sleep(0.1)
        assert hub._downstream_subscriptions == {}
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await hub.close()


@pytest.mark.asyncio
async def test_ps_publish_run_publishes_message_repeat_count_times():
    hub = PubSubNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    subscriber = PubSubClient()
    received = []
    subscriber.register_publish_callback(
        lambda subject, payload: received.append((subject, payload))
    )
    try:
        await subscriber.connect("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b")
        await asyncio.sleep(0.05)

        await ps_publish.run(
            "127.0.0.1", hub_port, "a.b", "hi", repeat_count=3, repeat_interval=0.05
        )
        await asyncio.sleep(0.1)

        assert received == [("a.b", b"hi")] * 3
    finally:
        await subscriber.close()
        await hub.close()


@pytest.mark.asyncio
async def test_ps_publish_run_without_subject_or_message_does_not_publish():
    hub = PubSubNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    subscriber = PubSubClient()
    received = []
    subscriber.register_publish_callback(
        lambda subject, payload: received.append((subject, payload))
    )
    try:
        await subscriber.connect("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)
        await subscriber.subscribe("a.b")
        await asyncio.sleep(0.05)

        await ps_publish.run("127.0.0.1", hub_port, None, None)
        await asyncio.sleep(0.1)

        assert received == []
    finally:
        await subscriber.close()
        await hub.close()

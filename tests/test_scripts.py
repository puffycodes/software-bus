import argparse
import asyncio

import pytest

from software_bus import bl_client, bl_server
from software_bus._cli import parse_address
from software_bus.base_layer import DEFAULT_HOST, DEFAULT_PORT, BaseLayerNode

from helpers import accept_one_peer_connection, free_port, open_peer_connection


def test_parse_address_valid():
    assert parse_address("127.0.0.1:8787") == ("127.0.0.1", 8787)


@pytest.mark.parametrize("value", ["no-port", ":8787", "127.0.0.1:", "127.0.0.1:abc"])
def test_parse_address_rejects_malformed_input(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_address(value)


def test_bl_server_arg_parser_defaults_to_no_addresses():
    args = bl_server.build_arg_parser().parse_args([])
    assert args.upstream == []
    assert args.listen == []
    assert args.debug is False


def test_bl_server_arg_parser_parses_debug_flag():
    args = bl_server.build_arg_parser().parse_args(["--debug", "true"])
    assert args.debug is True


def test_bl_server_arg_parser_accumulates_repeated_flags():
    args = bl_server.build_arg_parser().parse_args(
        [
            "--upstream",
            "10.0.0.1:1111",
            "--upstream",
            "10.0.0.2:2222",
            "--listen",
            "0.0.0.0:3333",
        ]
    )
    assert args.upstream == [("10.0.0.1", 1111), ("10.0.0.2", 2222)]
    assert args.listen == [("0.0.0.0", 3333)]


def test_bl_client_arg_parser_defaults_match_base_layer_defaults():
    args = bl_client.build_arg_parser().parse_args(["--message", "hi"])
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)
    assert args.message == "hi"
    assert args.repeat_count == 1
    assert args.repeat_interval == 1.0


def test_bl_client_arg_parser_custom_repeat_options():
    args = bl_client.build_arg_parser().parse_args(
        ["--message", "hi", "--repeat-count", "3", "--repeat-interval", "0.5"]
    )
    assert args.repeat_count == 3
    assert args.repeat_interval == 0.5


def test_bl_client_arg_parser_message_defaults_to_none():
    args = bl_client.build_arg_parser().parse_args([])
    assert args.message is None


@pytest.mark.asyncio
async def test_bl_server_run_listens_and_connects_upstream():
    upstream_server, upstream_connected = await accept_one_peer_connection()
    upstream_port = upstream_server.sockets[0].getsockname()[1]
    listen_port = free_port()

    task = asyncio.ensure_future(
        bl_server.run([("127.0.0.1", upstream_port)], [("127.0.0.1", listen_port)])
    )
    downstream_connection = None
    try:
        upstream_peer = await asyncio.wait_for(upstream_connected, timeout=1)
        await asyncio.sleep(0.05)  # let the server task reach accept_connection

        downstream_connection = await open_peer_connection("127.0.0.1", listen_port)
        await asyncio.sleep(0.05)

        await downstream_connection.send(b"ping")
        received = await asyncio.wait_for(upstream_peer.receive(), timeout=1)
        assert received == b"ping"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if downstream_connection is not None:
            await downstream_connection.close()
        upstream_server.close()
        await upstream_server.wait_closed()


@pytest.mark.asyncio
async def test_bl_client_run_sends_message_and_prints_received(capsys):
    hub = BaseLayerNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    other_peer = await open_peer_connection("127.0.0.1", hub_port)

    task = asyncio.ensure_future(bl_client.run("127.0.0.1", hub_port, "hello"))
    try:
        received = await asyncio.wait_for(other_peer.receive(), timeout=1)
        assert received == b"hello"

        await other_peer.send(b"reply")
        await asyncio.sleep(0.1)

        captured = capsys.readouterr()
        assert "reply" in captured.out
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await other_peer.close()
        await hub.close()


@pytest.mark.asyncio
async def test_bl_client_run_without_message_does_not_send_but_still_listens(capsys):
    hub = BaseLayerNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    other_peer = await open_peer_connection("127.0.0.1", hub_port)

    task = asyncio.ensure_future(bl_client.run("127.0.0.1", hub_port, None))
    try:
        await asyncio.sleep(0.05)

        # nothing should have been sent
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(other_peer.receive(), timeout=0.2)

        # it should still relay incoming data to the registered callback
        await other_peer.send(b"still listening")
        await asyncio.sleep(0.1)

        captured = capsys.readouterr()
        assert "still listening" in captured.out
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await other_peer.close()
        await hub.close()


@pytest.mark.asyncio
async def test_bl_client_run_sends_message_repeat_count_times():
    hub = BaseLayerNode()
    server = await hub.accept_connection(port=0)
    hub_port = server.sockets[0].getsockname()[1]

    other_peer = await open_peer_connection("127.0.0.1", hub_port)

    task = asyncio.ensure_future(
        bl_client.run(
            "127.0.0.1", hub_port, "hi", repeat_count=3, repeat_interval=0.05
        )
    )
    try:
        received = [
            await asyncio.wait_for(other_peer.receive(), timeout=1) for _ in range(3)
        ]
        assert received == [b"hi", b"hi", b"hi"]

        # no fourth send should follow
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(other_peer.receive(), timeout=0.2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await other_peer.close()
        await hub.close()

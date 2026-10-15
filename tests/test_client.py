import asyncio
import inspect
import logging

import pytest

from software_bus import BaseLayerNode, BaseLayerClient
from software_bus.base_layer import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    MAX_PAYLOAD_LENGTH,
    ConnectionTooSlow,
    PayloadTooLong,
    ReceivedTooLong,
)

from helpers import accept_one_peer_connection, wait_until


def test_client_instantiation_defaults():
    client = BaseLayerClient()
    assert client.connection is None


def test_client_connect_defaults_match_base_layer_defaults():
    signature = inspect.signature(BaseLayerClient.connect)
    assert signature.parameters["host"].default == DEFAULT_HOST
    assert signature.parameters["port"].default == DEFAULT_PORT


@pytest.mark.asyncio
async def test_client_connect_sets_connection():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]

        connection = await client.connect("127.0.0.1", bound_port)

        assert client.connection is connection
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_connect_while_connected_raises_and_keeps_the_first_connection():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    received = []
    hub.register_downstream_receive_callback(lambda source, data: received.append(data))
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        first = await client.connect("127.0.0.1", bound_port)

        with pytest.raises(RuntimeError, match="already connected"):
            await client.connect("127.0.0.1", bound_port)

        assert client.connection is first
        await client.send(b"still here")
        await asyncio.sleep(0.05)
        assert received == [b"still here"]
        assert len(hub.downstream_connections) == 1  # no second connection was opened
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_connects_again_after_close():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    received = []
    hub.register_downstream_receive_callback(lambda source, data: received.append(data))
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        await client.connect("127.0.0.1", bound_port)
        await client.close()

        second = await client.connect("127.0.0.1", bound_port)
        assert client.connection is second
        await client.send(b"again")
        await asyncio.sleep(0.05)
        assert received == [b"again"]
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_two_connects_at_once_leave_one_connection():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]

        results = await asyncio.gather(
            client.connect("127.0.0.1", bound_port),
            client.connect("127.0.0.1", bound_port),
            return_exceptions=True,
        )

        connections = [r for r in results if not isinstance(r, BaseException)]
        assert len(connections) == 1 and client.connection is connections[0]
        assert any(isinstance(r, RuntimeError) for r in results)
        await asyncio.sleep(0.05)
        assert len(hub.downstream_connections) == 1  # the loser's connection was closed
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_send_reaches_hub():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]

        await client.connect("127.0.0.1", bound_port)
        await asyncio.sleep(0.05)

        await client.send(b"ping")
        await asyncio.sleep(0.05)

        assert len(hub.downstream_connections) == 1
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_receive_callback_invoked_with_data():
    hub = BaseLayerNode()
    sender = BaseLayerClient()
    receiver = BaseLayerClient()
    received = []
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]

        await sender.connect("127.0.0.1", bound_port)
        await receiver.connect("127.0.0.1", bound_port)
        receiver.register_receive_callback(received.append)
        await asyncio.sleep(0.05)

        await sender.send(b"hello")
        await asyncio.sleep(0.1)

        assert received == [b"hello"]
    finally:
        await sender.close()
        await receiver.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_receive_callback_supports_async_callback():
    hub = BaseLayerNode()
    sender = BaseLayerClient()
    receiver = BaseLayerClient()
    received = []

    async def async_callback(data):
        received.append(data)

    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]

        await sender.connect("127.0.0.1", bound_port)
        await receiver.connect("127.0.0.1", bound_port)
        receiver.register_receive_callback(async_callback)
        await asyncio.sleep(0.05)

        await sender.send(b"hello-async")
        await asyncio.sleep(0.1)

        assert received == [b"hello-async"]
    finally:
        await sender.close()
        await receiver.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_no_callback_by_default_does_not_raise():
    hub = BaseLayerNode()
    sender = BaseLayerClient()
    receiver = BaseLayerClient()
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]

        await sender.connect("127.0.0.1", bound_port)
        await receiver.connect("127.0.0.1", bound_port)
        await asyncio.sleep(0.05)

        await sender.send(b"no callback registered")
        await asyncio.sleep(0.1)
    finally:
        await sender.close()
        await receiver.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_send_without_connect_raises():
    client = BaseLayerClient()
    with pytest.raises(RuntimeError):
        await client.send(b"data")


@pytest.mark.asyncio
async def test_client_receive_loop_ends_quietly_on_os_error(monkeypatch):
    hub = BaseLayerNode()
    client = BaseLayerClient()
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        connection = await client.connect("127.0.0.1", bound_port)
        connection.background_task.cancel()

        async def failing_receive():
            raise OSError("socket failure that is not a ConnectionError")

        monkeypatch.setattr(connection, "receive", failing_receive)

        # Must return normally rather than propagate the OSError.
        await client._receive_loop(connection)
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_connection_error_callback_called_when_peer_drops():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    errors = []
    client.register_connection_error_callback(
        lambda connection, error: errors.append((connection, error))
    )
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        connection = await client.connect("127.0.0.1", bound_port)
        await asyncio.sleep(0.05)

        await hub.close()
        await asyncio.sleep(0.1)

        assert len(errors) == 1
        assert errors[0][0] is connection
        assert errors[0][1] is not None
        assert client.connection is None
        with pytest.raises(RuntimeError):
            await client.send(b"too late")
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_default_connection_error_callback_logs(caplog):
    hub = BaseLayerNode()
    client = BaseLayerClient()
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        await client.connect("127.0.0.1", bound_port)
        await asyncio.sleep(0.05)

        with caplog.at_level(logging.INFO, logger="software_bus.client"):
            await hub.close()
            await asyncio.sleep(0.1)

        assert any("has error" in record.getMessage() for record in caplog.records)
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_close_does_not_call_connection_error_callback():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    errors = []
    client.register_connection_error_callback(
        lambda connection, error: errors.append(error)
    )
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        await client.connect("127.0.0.1", bound_port)
        await asyncio.sleep(0.05)

        await client.close()
        await asyncio.sleep(0.1)

        assert errors == []
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_send_failure_reported_to_callback_and_raised():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    errors = []
    client.register_connection_error_callback(
        lambda connection, error: errors.append(error)
    )
    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        connection = await client.connect("127.0.0.1", bound_port)

        failure = ConnectionResetError("simulated send failure")

        async def failing_send(data):
            raise failure

        connection.send = failing_send

        with pytest.raises(ConnectionResetError):
            await client.send(b"data")

        assert errors == [failure]
        assert client.connection is None
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_failing_receive_callback_costs_only_that_message(caplog):
    hub = BaseLayerNode()
    sender = BaseLayerClient()
    receiver = BaseLayerClient()
    received = []
    errors = []

    def callback(data):
        received.append(data)
        if data == b"first":
            raise ValueError("bad callback")

    try:
        server = await hub.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        await sender.connect("127.0.0.1", bound_port)
        await receiver.connect("127.0.0.1", bound_port)
        receiver.register_receive_callback(callback)
        receiver.register_connection_error_callback(lambda c, e: errors.append(e))
        await asyncio.sleep(0.05)

        with caplog.at_level(logging.ERROR, logger="software_bus.client"):
            await sender.send(b"first")
            await asyncio.sleep(0.05)
            await sender.send(b"second")
            await asyncio.sleep(0.1)

        assert received == [b"first", b"second"]
        assert errors == []
        assert receiver.connection is not None
        assert "bad callback" in caplog.text
    finally:
        await sender.close()
        await receiver.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_close_without_connecting_or_twice_is_harmless():
    hub = BaseLayerNode()
    client = BaseLayerClient()
    try:
        await client.close()  # never connected
        assert client.connection is None

        server = await hub.accept_connection(port=0)
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        await client.close()
        await asyncio.wait_for(client.close(), timeout=1)
        assert client.connection is None
    finally:
        await hub.close()


@pytest.mark.asyncio
async def test_client_send_over_the_size_limit_raises_and_keeps_the_connection():
    server, connected = await accept_one_peer_connection()
    client = BaseLayerClient()
    errors = []
    client.register_connection_error_callback(lambda connection, error: errors.append(error))
    try:
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        peer = await asyncio.wait_for(connected, timeout=1)

        with pytest.raises(PayloadTooLong):
            await client.send(b"x" * (MAX_PAYLOAD_LENGTH + 1))
        assert errors == [] and client.connection is not None

        payload = b"y" * MAX_PAYLOAD_LENGTH  # exactly at the limit is fine
        received = asyncio.ensure_future(peer.receive())  # read while it's sent
        await client.send(payload)
        assert await asyncio.wait_for(received, timeout=10) == payload
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_client_send_to_a_node_that_stops_reading_fails_after_the_send_timeout():
    server, connected = await accept_one_peer_connection()  # never reads
    client = BaseLayerClient()
    client.send_timeout = 0.3
    errors = []
    client.register_connection_error_callback(lambda connection, error: errors.append(error))
    peer = None
    try:
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        peer = await asyncio.wait_for(connected, timeout=1)

        with pytest.raises(ConnectionTooSlow):
            for _ in range(256):  # until the socket buffers are full
                await client.send(b"x" * (1024 * 1024))
        assert len(errors) == 1 and isinstance(errors[0], ConnectionTooSlow)
        assert client.connection is None
    finally:
        await client.close()
        if peer is not None:
            peer.writer.transport.abort()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_client_receiving_a_message_over_the_size_limit_loses_the_connection():
    server, connected = await accept_one_peer_connection()
    client = BaseLayerClient()
    lost = asyncio.get_event_loop().create_future()
    client.register_connection_error_callback(lambda connection, error: lost.set_result(error))
    try:
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        peer = await asyncio.wait_for(connected, timeout=1)
        peer.writer.write((MAX_PAYLOAD_LENGTH + 1).to_bytes(4, "big"))  # no payload follows
        await peer.writer.drain()

        assert isinstance(await asyncio.wait_for(lost, timeout=1), ReceivedTooLong)
        assert client.connection is None
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


def _raise(*args):
    raise RuntimeError("callback bug")


@pytest.mark.asyncio
async def test_client_failing_connection_error_callback_still_raises_the_send_error(caplog):
    hub = BaseLayerNode()
    client = BaseLayerClient()
    client.register_connection_error_callback(_raise)
    try:
        server = await hub.accept_connection(port=0)
        connection = await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])

        async def failing_send(data):
            raise ConnectionResetError("simulated send failure")

        connection.send = failing_send
        with caplog.at_level(logging.ERROR, logger="software_bus.base_layer"):
            with pytest.raises(ConnectionResetError):  # the error of the send, not the callback
                await client.send(b"data")
        assert client.connection is None
        assert connection.writer.is_closing()
        assert "Connection error callback failed" in caplog.text
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_failing_connection_error_callback_stays_out_of_the_receive_loop(caplog):
    hub = BaseLayerNode()
    client = BaseLayerClient()
    client.register_connection_error_callback(_raise)
    try:
        server = await hub.accept_connection(port=0)
        connection = await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        with caplog.at_level(logging.ERROR, logger="software_bus.base_layer"):
            await hub.close()  # the node goes away
            await asyncio.wait_for(asyncio.wait({connection.background_task}), timeout=1)
        assert client.connection is None
        assert connection.background_task.exception() is None
        assert "Connection error callback failed" in caplog.text
    finally:
        await client.close()
        await hub.close()


@pytest.mark.asyncio
async def test_client_close_does_not_hang_on_a_node_that_stopped_reading():
    server, connected = await accept_one_peer_connection()  # never reads
    client = BaseLayerClient()
    client.send_timeout = 60  # so the sends below block instead of failing
    peer = None
    try:
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        peer = await asyncio.wait_for(connected, timeout=1)

        async def flood():
            while True:
                await client.send(b"x" * (1024 * 1024))

        sender = asyncio.ensure_future(flood())
        await wait_until(lambda: client.connection.writer.transport.get_write_buffer_size() > 0)
        await asyncio.sleep(0.2)  # stuck: the socket buffers are full
        sender.cancel()
        client.close_timeout = 0.3  # how long close() may wait for the rest to be taken

        await asyncio.wait_for(client.close(), timeout=2)
        assert client.connection is None
    finally:
        await client.close()
        if peer is not None:
            peer.writer.transport.abort()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_client_close_still_delivers_what_was_sent_just_before():
    server, connected = await accept_one_peer_connection()
    client = BaseLayerClient()
    try:
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
        peer = await asyncio.wait_for(connected, timeout=1)
        for i in range(20):
            await client.send(b"message %d" % i)
        await client.close()  # straight away: these must not be dropped

        for i in range(20):
            assert await asyncio.wait_for(peer.receive(), timeout=1) == b"message %d" % i
    finally:
        await client.close()
        server.close()
        await server.wait_closed()

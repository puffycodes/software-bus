import asyncio
import inspect
import logging

import pytest

from software_bus import BaseLayerNode, BaseLayerClient
from software_bus.base_layer import DEFAULT_HOST, DEFAULT_PORT


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

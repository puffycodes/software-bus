import asyncio
import inspect

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

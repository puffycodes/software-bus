import asyncio

import pytest

from software_bus import BaseLayerNode

from helpers import accept_one_peer_connection, open_peer_connection

_open_peer_connection = open_peer_connection
_accept_one_peer_connection = accept_one_peer_connection


def test_instantiation_defaults():
    layer = BaseLayerNode()
    assert layer.listening_addresses == []
    assert layer.upstream_connections == []
    assert layer.downstream_connections == []
    assert not layer.is_accepting()


@pytest.mark.asyncio
async def test_accept_connection_starts_listening_with_defaults():
    layer = BaseLayerNode()
    try:
        server = await layer.accept_connection(port=0)
        bound_port = server.sockets[0].getsockname()[1]
        assert layer.is_accepting(port=bound_port)
        assert ("127.0.0.1", bound_port) in layer.listening_addresses
    finally:
        await layer.close()
    assert layer.listening_addresses == []


@pytest.mark.asyncio
async def test_accept_connection_is_idempotent_per_address():
    layer = BaseLayerNode()
    try:
        server1 = await layer.accept_connection(host="127.0.0.1", port=0)
        bound_port = server1.sockets[0].getsockname()[1]
        server2 = await layer.accept_connection(host="127.0.0.1", port=bound_port)
        assert server1 is server2
        assert layer.listening_addresses.count(("127.0.0.1", bound_port)) == 1
    finally:
        await layer.close()


@pytest.mark.asyncio
async def test_accept_connection_supports_multiple_addresses():
    layer = BaseLayerNode()
    try:
        server1 = await layer.accept_connection(host="127.0.0.1", port=0)
        server2 = await layer.accept_connection(host="127.0.0.1", port=0)
        port1 = server1.sockets[0].getsockname()[1]
        port2 = server2.sockets[0].getsockname()[1]

        assert port1 != port2
        assert len(layer.listening_addresses) == 2
        assert layer.is_accepting(port=port1)
        assert layer.is_accepting(port=port2)
    finally:
        await layer.close()


@pytest.mark.asyncio
async def test_establish_connection_between_two_instances():
    downstream_layer = BaseLayerNode()
    server = await downstream_layer.accept_connection(port=0)
    bound_port = server.sockets[0].getsockname()[1]

    upstream_layer = BaseLayerNode()
    try:
        connection = await upstream_layer.establish_connection("127.0.0.1", bound_port)

        assert connection in upstream_layer.upstream_connections
        assert len(upstream_layer.upstream_connections) == 1

        # give the server loop a chance to register the accepted connection
        await asyncio.sleep(0.05)
        assert len(downstream_layer.downstream_connections) == 1
    finally:
        await upstream_layer.close()
        await downstream_layer.close()


@pytest.mark.asyncio
async def test_close_clears_connection_lists_and_addresses():
    downstream_layer = BaseLayerNode()
    server = await downstream_layer.accept_connection(port=0)
    bound_port = server.sockets[0].getsockname()[1]

    upstream_layer = BaseLayerNode()
    await upstream_layer.establish_connection("127.0.0.1", bound_port)
    await asyncio.sleep(0.05)

    await upstream_layer.close()
    await downstream_layer.close()

    assert upstream_layer.upstream_connections == []
    assert downstream_layer.downstream_connections == []
    assert downstream_layer.listening_addresses == []


@pytest.mark.asyncio
async def test_async_context_manager():
    async with BaseLayerNode() as layer:
        assert layer.is_accepting()
    assert layer.listening_addresses == []


@pytest.mark.asyncio
async def test_data_from_downstream_relayed_to_upstream_and_other_downstream():
    hub = BaseLayerNode()
    upstream_server, upstream_connected = await _accept_one_peer_connection()
    upstream_port = upstream_server.sockets[0].getsockname()[1]
    peer_a = peer_b = None
    try:
        server = await hub.accept_connection(port=0)
        hub_port = server.sockets[0].getsockname()[1]

        await hub.establish_connection("127.0.0.1", upstream_port)
        upstream_peer = await asyncio.wait_for(upstream_connected, timeout=1)

        peer_a = await _open_peer_connection("127.0.0.1", hub_port)
        peer_b = await _open_peer_connection("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)  # let the hub register both downstream connections

        await peer_a.send(b"hello")

        assert await asyncio.wait_for(peer_b.receive(), timeout=1) == b"hello"
        assert await asyncio.wait_for(upstream_peer.receive(), timeout=1) == b"hello"

        # the sender must not get its own message echoed back
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer_a.receive(), timeout=0.2)
    finally:
        await hub.close()
        upstream_server.close()
        await upstream_server.wait_closed()
        if peer_a is not None:
            await peer_a.close()
        if peer_b is not None:
            await peer_b.close()


@pytest.mark.asyncio
async def test_data_from_upstream_relayed_only_to_downstream():
    hub = BaseLayerNode()
    servers = []
    downstream_peer = None
    try:
        server = await hub.accept_connection(port=0)
        hub_port = server.sockets[0].getsockname()[1]

        upstream_server_1, upstream_connected_1 = await _accept_one_peer_connection()
        upstream_server_2, upstream_connected_2 = await _accept_one_peer_connection()
        servers.extend([upstream_server_1, upstream_server_2])

        await hub.establish_connection(
            "127.0.0.1", upstream_server_1.sockets[0].getsockname()[1]
        )
        await hub.establish_connection(
            "127.0.0.1", upstream_server_2.sockets[0].getsockname()[1]
        )
        upstream_peer_1 = await asyncio.wait_for(upstream_connected_1, timeout=1)
        upstream_peer_2 = await asyncio.wait_for(upstream_connected_2, timeout=1)

        downstream_peer = await _open_peer_connection("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)

        await upstream_peer_1.send(b"from-upstream")

        assert (
            await asyncio.wait_for(downstream_peer.receive(), timeout=1)
            == b"from-upstream"
        )

        # must not be relayed to other upstream connections
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(upstream_peer_2.receive(), timeout=0.2)
    finally:
        await hub.close()
        for server in servers:
            server.close()
            await server.wait_closed()
        if downstream_peer is not None:
            await downstream_peer.close()


@pytest.mark.asyncio
async def test_register_downstream_receive_callback_overrides_default_relay():
    hub = BaseLayerNode()
    received = []
    hub.register_downstream_receive_callback(
        lambda source, data: received.append(data)
    )
    peer_a = peer_b = None
    try:
        server = await hub.accept_connection(port=0)
        hub_port = server.sockets[0].getsockname()[1]

        peer_a = await _open_peer_connection("127.0.0.1", hub_port)
        peer_b = await _open_peer_connection("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)

        await peer_a.send(b"hello")
        await asyncio.sleep(0.05)

        assert received == [b"hello"]
        # the default relay behaviour must not have also run
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer_b.receive(), timeout=0.2)
    finally:
        await hub.close()
        if peer_a is not None:
            await peer_a.close()
        if peer_b is not None:
            await peer_b.close()


@pytest.mark.asyncio
async def test_register_upstream_receive_callback_overrides_default_relay():
    hub = BaseLayerNode()
    received = []

    async def async_callback(source, data):
        received.append(data)

    hub.register_upstream_receive_callback(async_callback)
    upstream_server, upstream_connected = await _accept_one_peer_connection()
    upstream_port = upstream_server.sockets[0].getsockname()[1]
    downstream_peer = None
    try:
        server = await hub.accept_connection(port=0)
        hub_port = server.sockets[0].getsockname()[1]

        await hub.establish_connection("127.0.0.1", upstream_port)
        upstream_peer = await asyncio.wait_for(upstream_connected, timeout=1)

        downstream_peer = await _open_peer_connection("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)

        await upstream_peer.send(b"from-upstream")
        await asyncio.sleep(0.05)

        assert received == [b"from-upstream"]
        # the default relay behaviour must not have also run
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(downstream_peer.receive(), timeout=0.2)
    finally:
        await hub.close()
        upstream_server.close()
        await upstream_server.wait_closed()
        if downstream_peer is not None:
            await downstream_peer.close()


@pytest.mark.asyncio
async def test_default_downstream_receive_callback_excludes_source_connection():
    hub = BaseLayerNode()
    peer_a = peer_b = None
    try:
        server = await hub.accept_connection(port=0)
        hub_port = server.sockets[0].getsockname()[1]

        peer_a = await _open_peer_connection("127.0.0.1", hub_port)
        peer_b = await _open_peer_connection("127.0.0.1", hub_port)
        await asyncio.sleep(0.05)

        source_connection = hub.downstream_connections[0]
        await hub._default_downstream_receive_callback(source_connection, b"hi")

        assert await asyncio.wait_for(peer_b.receive(), timeout=1) == b"hi"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer_a.receive(), timeout=0.2)
    finally:
        await hub.close()
        if peer_a is not None:
            await peer_a.close()
        if peer_b is not None:
            await peer_b.close()

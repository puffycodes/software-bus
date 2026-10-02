"""Cycle prevention between PubSubNodes (docs/design/publish-subscribe.md,
"Cycle Prevention"): a node refuses an established connection that would
close a cycle."""
import asyncio
import logging
import uuid

import pytest

from software_bus import BaseLayerNode, PubSubClient, PubSubNode
from software_bus._cli import ConnectFailed
from software_bus.pubsub import (
    CycleCheckRefused,
    HelloMessage,
    PublishMessage,
    ReachabilityQueryMessage,
    ReachabilityReplyMessage,
    ReachabilityResult,
    SubscriptionMessage,
    decode_message,
    encode_message,
)
from software_bus import ps_server

from helpers import (
    accept_one_peer_connection,
    answer_query,
    establish_to_raw_peer,
    expect_hello,
    greet_as_node,
    open_peer_connection,
)


async def _start_node():
    node = PubSubNode()
    server = await node.accept_connection(port=0)
    return node, server.sockets[0].getsockname()[1]


async def _receive(peer, timeout=1):
    return decode_message(await asyncio.wait_for(peer.receive(), timeout=timeout))


# --- wire format ---------------------------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        HelloMessage(bytes(range(16))),
        ReachabilityQueryMessage(bytes(16), bytes(range(16, 32))),
        ReachabilityReplyMessage(bytes(range(16)), ReachabilityResult.NOT_FOUND),
        ReachabilityReplyMessage(bytes(range(16)), ReachabilityResult.FOUND),
        ReachabilityReplyMessage(bytes(range(16)), ReachabilityResult.UNKNOWN),
    ],
)
def test_cycle_prevention_messages_roundtrip(message):
    assert decode_message(encode_message(message)) == message


def test_cycle_prevention_messages_wire_layout():
    node_id, query_id = bytes(range(16)), bytes(range(16, 32))
    assert encode_message(HelloMessage(node_id)) == b"\x03" + node_id
    assert encode_message(ReachabilityQueryMessage(query_id, node_id)) == b"\x04" + query_id + node_id
    assert (
        encode_message(ReachabilityReplyMessage(query_id, ReachabilityResult.UNKNOWN))
        == b"\x05" + query_id + b"\x02"
    )


@pytest.mark.parametrize(
    "data",
    [
        b"\x03" + bytes(15),  # hello too short
        b"\x03" + bytes(17),  # hello too long
        b"\x04" + bytes(32 - 1),  # query too short
        b"\x05" + bytes(16),  # reply without a result
        b"\x05" + bytes(16) + b"\x03",  # result not 0, 1 or 2
    ],
)
def test_decode_malformed_cycle_prevention_message_raises(data):
    with pytest.raises(ValueError):
        decode_message(data)


def test_encode_rejects_ids_of_wrong_length():
    with pytest.raises(ValueError):
        encode_message(HelloMessage(b"short"))


def test_every_node_has_its_own_16_byte_node_id():
    first, second = PubSubNode(), PubSubNode()
    assert len(first.node_id) == 16
    assert first.node_id != second.node_id


# --- accepting and refusing connections ------------------------------------------


@pytest.mark.asyncio
async def test_connection_between_two_nodes_is_accepted():
    parent, parent_port = await _start_node()
    child = PubSubNode()
    try:
        await asyncio.wait_for(child.establish_connection("127.0.0.1", parent_port), timeout=2)
        await asyncio.sleep(0.05)

        assert len(child.upstream_connections) == 1
        assert child._active_node_connections() == child.upstream_connections
        assert parent._active_node_connections() == parent.downstream_connections
    finally:
        await child.close()
        await parent.close()


@pytest.mark.asyncio
async def test_connection_closing_a_triangle_is_refused():
    root, root_port = await _start_node()
    left, left_port = await _start_node()
    right = PubSubNode()
    try:
        await left.establish_connection("127.0.0.1", root_port)
        await right.establish_connection("127.0.0.1", root_port)

        with pytest.raises(CycleCheckRefused, match="would create a cycle"):
            await asyncio.wait_for(right.establish_connection("127.0.0.1", left_port), timeout=5)
        await asyncio.sleep(0.05)

        assert len(right.upstream_connections) == 1  # only the link to root is left
        assert left.downstream_connections == []  # the refused link is closed on both ends
    finally:
        await right.close()
        await left.close()
        await root.close()


@pytest.mark.asyncio
async def test_connection_closing_a_longer_cycle_is_refused():
    # a chain a <- b <- c <- d; connecting a to d closes a cycle of four
    nodes = [await _start_node() for _ in range(4)]
    try:
        for (child, _), (_, parent_port) in zip(nodes[1:], nodes[:-1]):
            await child.establish_connection("127.0.0.1", parent_port)

        first, _ = nodes[0]
        _, last_port = nodes[-1]
        with pytest.raises(CycleCheckRefused, match="would create a cycle"):
            await asyncio.wait_for(first.establish_connection("127.0.0.1", last_port), timeout=5)
    finally:
        for node, _ in nodes:
            await node.close()


@pytest.mark.asyncio
async def test_connection_to_itself_is_refused():
    node, port = await _start_node()
    try:
        with pytest.raises(CycleCheckRefused, match="itself"):
            await asyncio.wait_for(node.establish_connection("127.0.0.1", port), timeout=5)
    finally:
        await node.close()


@pytest.mark.asyncio
async def test_second_connection_to_the_same_node_is_refused():
    parent, parent_port = await _start_node()
    child = PubSubNode()
    try:
        await child.establish_connection("127.0.0.1", parent_port)
        with pytest.raises(CycleCheckRefused, match="already connected"):
            await asyncio.wait_for(child.establish_connection("127.0.0.1", parent_port), timeout=5)
        assert len(child.upstream_connections) == 1
    finally:
        await child.close()
        await parent.close()


@pytest.mark.asyncio
async def test_connection_to_a_peer_that_never_says_hello_is_refused():
    server, connected = await accept_one_peer_connection()
    node = PubSubNode()
    node.check_timeout = 0.2
    try:
        with pytest.raises(CycleCheckRefused, match="did not identify"):
            await asyncio.wait_for(
                node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1]),
                timeout=5,
            )
        assert node.upstream_connections == []
    finally:
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_connection_to_a_base_layer_node_is_refused():
    base = BaseLayerNode()
    server = await base.accept_connection(port=0)
    node = PubSubNode()
    node.check_timeout = 0.2
    try:
        with pytest.raises(CycleCheckRefused, match="did not identify"):
            await asyncio.wait_for(
                node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1]),
                timeout=5,
            )
    finally:
        await node.close()
        await base.close()


@pytest.mark.asyncio
async def test_check_that_keeps_getting_unknown_retries_then_refuses():
    node = PubSubNode()
    node.max_check_attempts = 3
    node.check_retry_delay = (0.01, 0.02)
    server_1, connected_1 = await accept_one_peer_connection()
    server_2, connected_2 = await accept_one_peer_connection()
    try:
        existing = await establish_to_raw_peer(node, server_1, connected_1)

        port_2 = server_2.sockets[0].getsockname()[1]
        task = asyncio.ensure_future(node.establish_connection("127.0.0.1", port_2))
        await greet_as_node(await asyncio.wait_for(connected_2, timeout=1))
        query_ids = set()
        for _ in range(3):
            query = await answer_query(existing, ReachabilityResult.UNKNOWN)
            query_ids.add(query.query_id)

        with pytest.raises(CycleCheckRefused, match="could not determine"):
            await asyncio.wait_for(task, timeout=5)
        assert len(query_ids) == 3  # a new query ID for every attempt
    finally:
        await node.close()
        for server in (server_1, server_2):
            server.close()
            await server.wait_closed()


@pytest.mark.asyncio
async def test_a_found_reply_refuses_at_once_without_waiting_for_the_others():
    node = PubSubNode()  # default check_timeout: a wait for the silent peer would take 5 s
    servers = [await accept_one_peer_connection() for _ in range(3)]
    try:
        (server_1, connected_1), (server_2, connected_2), (server_3, connected_3) = servers
        existing_1 = await establish_to_raw_peer(node, server_1, connected_1)
        existing_2 = await establish_to_raw_peer(node, server_2, connected_2, answering=[existing_1])

        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server_3.sockets[0].getsockname()[1])
        )
        await greet_as_node(await asyncio.wait_for(connected_3, timeout=1))
        query = await _receive(existing_1)
        assert isinstance(await _receive(existing_2), ReachabilityQueryMessage)  # never answered
        await existing_1.send(
            encode_message(ReachabilityReplyMessage(query.query_id, ReachabilityResult.FOUND))
        )

        with pytest.raises(CycleCheckRefused, match="would create a cycle"):
            await asyncio.wait_for(task, timeout=1)
    finally:
        await node.close()
        for server, _ in servers:
            server.close()
            await server.wait_closed()


@pytest.mark.asyncio
async def test_a_connection_that_fails_before_replying_counts_as_not_found():
    node = PubSubNode()  # default check_timeout: waiting it out would take 5 s
    servers = [await accept_one_peer_connection() for _ in range(3)]
    try:
        (server_1, connected_1), (server_2, connected_2), (server_3, connected_3) = servers
        existing_1 = await establish_to_raw_peer(node, server_1, connected_1)
        existing_2 = await establish_to_raw_peer(node, server_2, connected_2, answering=[existing_1])

        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server_3.sockets[0].getsockname()[1])
        )
        await greet_as_node(await asyncio.wait_for(connected_3, timeout=1))
        await answer_query(existing_1, ReachabilityResult.NOT_FOUND)
        assert isinstance(await _receive(existing_2), ReachabilityQueryMessage)
        await existing_2.close()  # fails without replying

        await asyncio.wait_for(task, timeout=1)  # accepted
        assert len(node.upstream_connections) == 2  # existing_1 and the new one
    finally:
        await node.close()
        for server, _ in servers:
            server.close()
            await server.wait_closed()


# --- the pending connection -------------------------------------------------------


@pytest.mark.asyncio
async def test_messages_from_a_pending_connection_are_held_until_it_is_accepted():
    server, connected = await accept_one_peer_connection()
    node = PubSubNode()
    try:
        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(peer)

        # sent before the peer identifies itself: must wait for the check
        await peer.send(encode_message(SubscriptionMessage("a.b", subscribe=True)))
        await asyncio.sleep(0.1)
        assert node._upstream_subscriptions == {}

        await peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.wait_for(task, timeout=5)
        await asyncio.sleep(0.05)
        assert list(node._upstream_subscriptions) == ["a.b"]
    finally:
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_closing_the_node_ends_a_check_still_waiting_for_hello():
    server, connected = await accept_one_peer_connection()
    node = PubSubNode()
    try:
        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(peer)  # never answered: the check waits for a Hello

        await node.close()
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(task, timeout=1)  # well before check_timeout
        assert node._pending == {}
    finally:
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_nothing_is_sent_on_a_pending_connection_until_it_is_accepted():
    node, port = await _start_node()
    server, connected = await accept_one_peer_connection()
    subscriber = PubSubClient()
    try:
        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        upstream_peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(upstream_peer)

        # a subscription made while the upstream connection is pending...
        await subscriber.connect("127.0.0.1", port)
        await subscriber.subscribe("a.b", lambda *m: None)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(upstream_peer.receive(), timeout=0.2)

        # ...is only sent once the connection is accepted
        await upstream_peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.wait_for(task, timeout=5)
        assert await _receive(upstream_peer) == SubscriptionMessage("a.b", subscribe=True)
    finally:
        await subscriber.close()
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_subscriptions_flow_both_ways_once_accepted():
    parent, parent_port = await _start_node()
    child, child_port = await _start_node()
    parent_subscriber = PubSubClient()
    child_publisher = PubSubClient()
    received = []
    try:
        # the parent has a subscriber before the child joins
        await parent_subscriber.connect("127.0.0.1", parent_port)
        await parent_subscriber.subscribe("a.b", lambda m, a, p: received.append(p))
        await asyncio.sleep(0.05)

        await child.establish_connection("127.0.0.1", parent_port)
        await child_publisher.connect("127.0.0.1", child_port)
        await asyncio.sleep(0.1)
        await child_publisher.publish("a.b", b"hello")
        await asyncio.sleep(0.1)

        assert received == [b"hello"]
    finally:
        await parent_subscriber.close()
        await child_publisher.close()
        await child.close()
        await parent.close()


@pytest.mark.asyncio
async def test_peer_dropping_during_the_check_fails_establish_connection():
    server, connected = await accept_one_peer_connection()
    node = PubSubNode()  # default check_timeout: the failure must not wait for it
    try:
        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(peer)

        await peer.close()

        with pytest.raises(ConnectionError):
            await asyncio.wait_for(task, timeout=1)
        assert node.upstream_connections == []
        assert node._pending == {}
    finally:
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_messages_held_from_a_refused_connection_are_discarded():
    node, port = await _start_node()
    server, connected = await accept_one_peer_connection()
    subscriber = PubSubClient()
    received = []
    try:
        await subscriber.connect("127.0.0.1", port)
        await subscriber.subscribe("a.b", lambda m, a, payload: received.append(payload))
        await asyncio.sleep(0.05)

        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(peer)
        await peer.send(encode_message(SubscriptionMessage("x.y", subscribe=True)))
        await peer.send(encode_message(PublishMessage("a.b", b"held")))
        await peer.send(encode_message(HelloMessage(node.node_id)))  # refused: it is itself

        with pytest.raises(CycleCheckRefused, match="itself"):
            await asyncio.wait_for(task, timeout=5)
        await asyncio.sleep(0.1)

        assert node._upstream_subscriptions == {}
        assert received == []
    finally:
        await subscriber.close()
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_a_connection_failing_while_its_held_messages_are_replayed_stops_the_replay():
    server, connected = await accept_one_peer_connection()
    node = PubSubNode()
    original_handle_message = node._handle_message

    async def fail_after_the_first(source, message, *, from_upstream):
        await original_handle_message(source, message, from_upstream=from_upstream)
        if message == SubscriptionMessage("first", subscribe=True):
            await node._base._handle_connection_error(
                source, True, ConnectionResetError("simulated failure")
            )

    node._handle_message = fail_after_the_first
    try:
        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(peer)
        await peer.send(encode_message(SubscriptionMessage("first", subscribe=True)))
        await peer.send(encode_message(SubscriptionMessage("second", subscribe=True)))
        await asyncio.sleep(0.05)
        await peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=5)

        # "first" went with the failed connection; "second" must not tag a dead connection
        assert node._upstream_subscriptions == {}
        assert node.upstream_connections == []
    finally:
        await node.close()
        server.close()
        await server.wait_closed()


# --- answering queries -----------------------------------------------------------


async def _node_with_raw_node_peer():
    """A node plus a raw downstream peer that has greeted it as a node."""
    node, port = await _start_node()
    peer = await open_peer_connection("127.0.0.1", port)
    await expect_hello(peer)
    await peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
    await asyncio.sleep(0.05)
    return node, port, peer


@pytest.mark.asyncio
async def test_query_for_the_node_itself_is_answered_found():
    node, _, peer = await _node_with_raw_node_peer()
    try:
        query_id = uuid.uuid4().bytes
        await peer.send(encode_message(ReachabilityQueryMessage(query_id, node.node_id)))
        assert await _receive(peer) == ReachabilityReplyMessage(query_id, ReachabilityResult.FOUND)
    finally:
        await peer.close()
        await node.close()


@pytest.mark.asyncio
async def test_query_with_nowhere_to_forward_is_answered_not_found():
    node, _, peer = await _node_with_raw_node_peer()
    try:
        query_id = uuid.uuid4().bytes
        await peer.send(encode_message(ReachabilityQueryMessage(query_id, uuid.uuid4().bytes)))
        assert await _receive(peer) == ReachabilityReplyMessage(
            query_id, ReachabilityResult.NOT_FOUND
        )
    finally:
        await peer.close()
        await node.close()


@pytest.mark.asyncio
async def test_query_is_forwarded_to_other_nodes_only_and_their_reply_passed_back():
    node, port, asking_peer = await _node_with_raw_node_peer()
    other_peer = await open_peer_connection("127.0.0.1", port)
    client = PubSubClient()  # a client must not be asked
    client_messages = []
    try:
        await expect_hello(other_peer)
        await other_peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await client.connect("127.0.0.1", port)
        client._client.register_receive_callback(lambda data: client_messages.append(data))
        await asyncio.sleep(0.05)

        query = ReachabilityQueryMessage(uuid.uuid4().bytes, uuid.uuid4().bytes)
        await asking_peer.send(encode_message(query))
        assert await _receive(other_peer) == query  # forwarded unchanged
        await other_peer.send(
            encode_message(ReachabilityReplyMessage(query.query_id, ReachabilityResult.FOUND))
        )

        assert await _receive(asking_peer) == ReachabilityReplyMessage(
            query.query_id, ReachabilityResult.FOUND
        )
        assert not any(
            isinstance(decode_message(data), ReachabilityQueryMessage) for data in client_messages
        )
    finally:
        await client.close()
        await other_peer.close()
        await asking_peer.close()
        await node.close()


@pytest.mark.asyncio
async def test_forwarded_query_that_times_out_is_answered_unknown():
    node, port, asking_peer = await _node_with_raw_node_peer()
    node.check_timeout = 0.2
    silent_peer = await open_peer_connection("127.0.0.1", port)
    try:
        await expect_hello(silent_peer)
        await silent_peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.sleep(0.05)

        query_id = uuid.uuid4().bytes
        await asking_peer.send(encode_message(ReachabilityQueryMessage(query_id, uuid.uuid4().bytes)))
        assert await _receive(asking_peer) == ReachabilityReplyMessage(
            query_id, ReachabilityResult.UNKNOWN
        )
    finally:
        await silent_peer.close()
        await asking_peer.close()
        await node.close()


@pytest.mark.asyncio
async def test_same_query_arriving_twice_is_answered_unknown(caplog):
    node, port, asking_peer = await _node_with_raw_node_peer()
    silent_peer = await open_peer_connection("127.0.0.1", port)
    try:
        await expect_hello(silent_peer)
        await silent_peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.sleep(0.05)

        query = ReachabilityQueryMessage(uuid.uuid4().bytes, uuid.uuid4().bytes)
        with caplog.at_level(logging.WARNING, logger="software_bus.pubsub"):
            await asking_peer.send(encode_message(query))  # forwarded, waits for silent_peer
            await asking_peer.send(encode_message(query))
            assert await _receive(asking_peer) == ReachabilityReplyMessage(
                query.query_id, ReachabilityResult.UNKNOWN
            )
        assert any("already a cycle" in record.getMessage() for record in caplog.records)
    finally:
        await silent_peer.close()
        await asking_peer.close()
        await node.close()


@pytest.mark.asyncio
async def test_query_on_a_pending_connection_is_answered_not_found_even_for_this_node():
    server, connected = await accept_one_peer_connection()
    node = PubSubNode()
    try:
        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        peer = await asyncio.wait_for(connected, timeout=1)
        await expect_hello(peer)

        # still pending (no Hello from the peer yet), and the node isn't checking
        query_id = uuid.uuid4().bytes
        await peer.send(encode_message(ReachabilityQueryMessage(query_id, node.node_id)))
        assert await _receive(peer) == ReachabilityReplyMessage(
            query_id, ReachabilityResult.NOT_FOUND
        )

        await peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.wait_for(task, timeout=5)
    finally:
        await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_node_answers_unknown_while_checking():
    node = PubSubNode()
    server_1, connected_1 = await accept_one_peer_connection()
    server_2, connected_2 = await accept_one_peer_connection()
    try:
        existing = await establish_to_raw_peer(node, server_1, connected_1)

        task = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server_2.sockets[0].getsockname()[1])
        )
        await greet_as_node(await asyncio.wait_for(connected_2, timeout=1))
        own_query = await _receive(existing)  # the node is now checking
        assert isinstance(own_query, ReachabilityQueryMessage)

        other_query_id = uuid.uuid4().bytes
        await existing.send(
            encode_message(ReachabilityQueryMessage(other_query_id, uuid.uuid4().bytes))
        )
        assert await _receive(existing) == ReachabilityReplyMessage(
            other_query_id, ReachabilityResult.UNKNOWN
        )

        await existing.send(
            encode_message(ReachabilityReplyMessage(own_query.query_id, ReachabilityResult.NOT_FOUND))
        )
        await asyncio.wait_for(task, timeout=5)
    finally:
        await node.close()
        for server in (server_1, server_2):
            server.close()
            await server.wait_closed()


@pytest.mark.asyncio
async def test_a_reply_arriving_after_the_result_is_known_is_ignored():
    node, port, asking_peer = await _node_with_raw_node_peer()
    node.check_timeout = 0.2
    slow_peer = await open_peer_connection("127.0.0.1", port)
    try:
        await expect_hello(slow_peer)
        await slow_peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))
        await asyncio.sleep(0.05)

        query = ReachabilityQueryMessage(uuid.uuid4().bytes, uuid.uuid4().bytes)
        await asking_peer.send(encode_message(query))
        assert await _receive(slow_peer) == query
        assert await _receive(asking_peer) == ReachabilityReplyMessage(
            query.query_id, ReachabilityResult.UNKNOWN
        )

        await slow_peer.send(
            encode_message(ReachabilityReplyMessage(query.query_id, ReachabilityResult.FOUND))
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asking_peer.receive(), timeout=0.2)
        assert node._queries == {}
    finally:
        await slow_peer.close()
        await asking_peer.close()
        await node.close()


@pytest.mark.asyncio
async def test_query_on_a_pending_connection_while_checking_is_answered_unknown():
    node = PubSubNode()
    servers = [await accept_one_peer_connection() for _ in range(3)]
    third = None
    try:
        (server_1, connected_1), (server_2, connected_2), (server_3, connected_3) = servers
        existing = await establish_to_raw_peer(node, server_1, connected_1)

        second = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server_2.sockets[0].getsockname()[1])
        )
        await greet_as_node(await asyncio.wait_for(connected_2, timeout=1))
        own_query = await _receive(existing)  # the node is now checking

        third = asyncio.ensure_future(
            node.establish_connection("127.0.0.1", server_3.sockets[0].getsockname()[1])
        )
        pending_peer = await asyncio.wait_for(connected_3, timeout=1)
        await expect_hello(pending_peer)  # pending, waiting for the first check to finish

        query_id = uuid.uuid4().bytes
        await pending_peer.send(
            encode_message(ReachabilityQueryMessage(query_id, uuid.uuid4().bytes))
        )
        assert await _receive(pending_peer) == ReachabilityReplyMessage(
            query_id, ReachabilityResult.UNKNOWN
        )

        await existing.send(
            encode_message(ReachabilityReplyMessage(own_query.query_id, ReachabilityResult.NOT_FOUND))
        )
        await asyncio.wait_for(second, timeout=5)
    finally:
        await node.close()
        if third is not None:
            await asyncio.gather(third, return_exceptions=True)
        for server, _ in servers:
            server.close()
            await server.wait_closed()


# --- links added at the same time ------------------------------------------------


@pytest.mark.asyncio
async def test_one_node_adding_two_links_at_once_checks_them_one_at_a_time():
    # b and c are connected; a connecting to both would close a cycle. a also
    # has a raw peer that holds on to queries, so a check lasts until it answers.
    a = PubSubNode()
    b, b_port = await _start_node()
    c, c_port = await _start_node()
    server, connected = await accept_one_peer_connection()
    try:
        await c.establish_connection("127.0.0.1", b_port)
        slow_peer = await establish_to_raw_peer(a, server, connected)

        both = asyncio.ensure_future(
            asyncio.gather(
                a.establish_connection("127.0.0.1", b_port),
                a.establish_connection("127.0.0.1", c_port),
                return_exceptions=True,
            )
        )
        first_query = await _receive(slow_peer)
        assert isinstance(first_query, ReachabilityQueryMessage)
        # the second link's check must not start while the first is still running
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(slow_peer.receive(), timeout=0.3)
        await slow_peer.send(
            encode_message(ReachabilityReplyMessage(first_query.query_id, ReachabilityResult.NOT_FOUND))
        )
        await answer_query(slow_peer)  # the second check's query

        results = await asyncio.wait_for(both, timeout=10)
        refused = [r for r in results if isinstance(r, CycleCheckRefused)]
        assert len(refused) == 1, results
        assert len(a.upstream_connections) == 2  # the slow peer, and one of b and c
    finally:
        for node in (a, b, c):
            await node.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_two_nodes_adding_links_at_the_same_time_cannot_close_a_cycle():
    # a-c and b-d are connected; a->b plus c->d together would close a-b-d-c-a
    a, a_port = await _start_node()
    b, b_port = await _start_node()
    c, c_port = await _start_node()
    d, d_port = await _start_node()
    for node in (a, b, c, d):
        node.check_retry_delay = (0.05, 0.5)
    try:
        await c.establish_connection("127.0.0.1", a_port)
        await d.establish_connection("127.0.0.1", b_port)

        results = await asyncio.wait_for(
            asyncio.gather(
                a.establish_connection("127.0.0.1", b_port),
                c.establish_connection("127.0.0.1", d_port),
                return_exceptions=True,
            ),
            timeout=30,
        )
        accepted = [r for r in results if not isinstance(r, BaseException)]
        refused = [r for r in results if isinstance(r, CycleCheckRefused)]
        assert len(accepted) == 1 and len(refused) == 1, results
    finally:
        for node in (a, b, c, d):
            await node.close()


# --- clients and ps_server -------------------------------------------------------


@pytest.mark.asyncio
async def test_client_ignores_hello_quietly(caplog):
    node, port = await _start_node()
    client = PubSubClient()
    try:
        with caplog.at_level(logging.INFO, logger="software_bus.pubsub"):
            await client.connect("127.0.0.1", port)
            await asyncio.sleep(0.1)
        assert not any("alformed" in record.getMessage() for record in caplog.records)
        assert node._active_node_connections() == []  # a client is not a node connection
    finally:
        await client.close()
        await node.close()


@pytest.mark.asyncio
async def test_ps_server_run_exits_when_an_upstream_is_refused():
    parent, parent_port = await _start_node()
    try:
        with pytest.raises(ConnectFailed, match="already connected"):
            await asyncio.wait_for(
                ps_server.run(
                    [("127.0.0.1", parent_port), ("127.0.0.1", parent_port)],
                    [("127.0.0.1", 0)],
                ),
                timeout=10,
            )
    finally:
        await parent.close()

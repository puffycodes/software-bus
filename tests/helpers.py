import asyncio
import socket
import uuid

from software_bus.base_layer import Connection
from software_bus.pubsub import (
    HelloMessage,
    ReachabilityQueryMessage,
    ReachabilityReplyMessage,
    ReachabilityResult,
    decode_message,
    encode_message,
)


async def open_peer_connection(host: str, port: int) -> Connection:
    reader, writer = await asyncio.open_connection(host, port)
    return Connection(reader, writer)


async def accept_one_peer_connection():
    """Start a raw server on an ephemeral port and return it plus a future
    for the first connection it accepts, to stand in for a peer instance."""
    connected: asyncio.Future = asyncio.get_event_loop().create_future()
    accepted = []

    async def handler(reader, writer):
        connection = Connection(reader, writer)
        accepted.append(connection)
        connected.set_result(connection)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)

    # Server.wait_closed() waits for every accepted connection to be closed
    # (Python 3.12.1+), so close the peer's side first, as a real node would.
    original_wait_closed = server.wait_closed

    async def wait_closed():
        for connection in accepted:
            await connection.close()
        await original_wait_closed()

    server.wait_closed = wait_closed
    return server, connected


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def expect_hello(peer: Connection) -> bytes:
    """Read the Hello a PubSubNode sends first on every new connection; return its Node ID."""
    message = decode_message(await asyncio.wait_for(peer.receive(), timeout=1))
    assert isinstance(message, HelloMessage), message
    return message.node_id


async def greet_as_node(peer: Connection) -> None:
    """Make a raw peer answer a PubSubNode's Hello like another node would, so
    the node's cycle check for its connection to `peer` can accept it."""
    await expect_hello(peer)
    await peer.send(encode_message(HelloMessage(uuid.uuid4().bytes)))


async def answer_query(
    peer: Connection, result: ReachabilityResult = ReachabilityResult.NOT_FOUND
) -> ReachabilityQueryMessage:
    """Make a raw peer that greeted a node as a node answer the next message,
    which must be a reachability query; return the query."""
    message = decode_message(await asyncio.wait_for(peer.receive(), timeout=1))
    assert isinstance(message, ReachabilityQueryMessage), message
    await peer.send(encode_message(ReachabilityReplyMessage(message.query_id, result)))
    return message


async def establish_to_raw_peer(node, server, connected, answering=()) -> Connection:
    """Have `node` establish a connection to the raw peer server `server` (from
    accept_one_peer_connection), greeting it as a node so the cycle check
    accepts it; return the peer's side of the connection.

    `answering` are raw peers already greeted as nodes: the check sends each
    of them a reachability query, which they answer "not found".
    """
    port = server.sockets[0].getsockname()[1]
    task = asyncio.ensure_future(node.establish_connection("127.0.0.1", port))
    peer = await asyncio.wait_for(connected, timeout=1)
    await greet_as_node(peer)
    for existing_peer in answering:
        await answer_query(existing_peer)
    await asyncio.wait_for(task, timeout=5)
    return peer

import asyncio
import socket

from software_bus.base_layer import Connection


async def open_peer_connection(host: str, port: int) -> Connection:
    reader, writer = await asyncio.open_connection(host, port)
    return Connection(reader, writer)


async def accept_one_peer_connection():
    """Start a raw server on an ephemeral port and return it plus a future
    for the first connection it accepts, to stand in for a peer instance."""
    connected: asyncio.Future = asyncio.get_event_loop().create_future()

    async def handler(reader, writer):
        connected.set_result(Connection(reader, writer))

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, connected


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

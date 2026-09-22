from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787

Address = Tuple[str, int]

# Messages are framed as a 4-byte big-endian length prefix followed by the
# payload, since TCP gives no message boundaries of its own.
_LENGTH_PREFIX_SIZE = 4


@dataclass
class Connection:
    """A single TCP connection to another instance."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    background_task: Optional[asyncio.Task] = None

    @property
    def address(self) -> Optional[Tuple]:
        return self.writer.get_extra_info("peername")

    async def send(self, data: bytes) -> None:
        self.writer.write(len(data).to_bytes(_LENGTH_PREFIX_SIZE, "big") + data)
        await self.writer.drain()

    async def receive(self) -> bytes:
        header = await self.reader.readexactly(_LENGTH_PREFIX_SIZE)
        length = int.from_bytes(header, "big")
        return await self.reader.readexactly(length)

    async def close(self) -> None:
        if self.background_task is not None:
            self.background_task.cancel()
        if self.writer.is_closing():
            return
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except ConnectionError:
            pass


ReceiveCallback = Callable[["Connection", bytes], Any]


class BaseLayerNode:
    """Base layer of the software bus.

    Accepts TCP connections from downstream instances on any number of
    IP/port pairs and can establish TCP connections to upstream instances,
    tracking both in separate connection lists. Data received on any
    connection is relayed onward per the base layer's routing rule.
    """

    def __init__(self) -> None:
        self.listening_addresses: List[Address] = []
        self.upstream_connections: List[Connection] = []
        self.downstream_connections: List[Connection] = []
        self._servers: Dict[Address, asyncio.AbstractServer] = {}
        self._upstream_receive_callback: ReceiveCallback = (
            self._default_upstream_receive_callback
        )
        self._downstream_receive_callback: ReceiveCallback = (
            self._default_downstream_receive_callback
        )

    def is_accepting(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> bool:
        return (host, port) in self._servers

    def register_upstream_receive_callback(self, callback: ReceiveCallback) -> None:
        """Set the function to call when data is received from an upstream connection."""
        self._upstream_receive_callback = callback

    def register_downstream_receive_callback(self, callback: ReceiveCallback) -> None:
        """Set the function to call when data is received from a downstream connection."""
        self._downstream_receive_callback = callback

    async def accept_connection(
        self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
    ) -> asyncio.AbstractServer:
        """Start accepting downstream connections on the given IP/port.

        Does nothing if already accepting connections on that IP/port.
        Each accepted connection is appended to `downstream_connections`.
        """
        requested_address = (host, port)
        existing_server = self._servers.get(requested_address)
        if existing_server is not None:
            return existing_server

        server = await asyncio.start_server(
            self._on_downstream_connected, host, port
        )
        bound_port = server.sockets[0].getsockname()[1]
        address = (host, bound_port)
        self._servers[address] = server
        self.listening_addresses.append(address)
        logger.info("Accepting downstream connections on %s:%s", host, bound_port)
        return server

    async def _on_downstream_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        connection = Connection(reader, writer)
        self.downstream_connections.append(connection)
        logger.info("Accepted downstream connection from %s", connection.address)
        connection.background_task = asyncio.ensure_future(
            self._relay_loop(connection, self.downstream_connections)
        )

    async def establish_connection(self, host: str, port: int) -> Connection:
        """Open a TCP connection to an upstream instance.

        The resulting connection is appended to `upstream_connections`.
        """
        reader, writer = await asyncio.open_connection(host, port)
        connection = Connection(reader, writer)
        self.upstream_connections.append(connection)
        logger.info("Established upstream connection to %s:%s", host, port)
        connection.background_task = asyncio.ensure_future(
            self._relay_loop(connection, self.upstream_connections)
        )
        return connection

    async def _relay_loop(
        self, source: Connection, source_list: List[Connection]
    ) -> None:
        from_upstream = source_list is self.upstream_connections
        callback = (
            self._upstream_receive_callback
            if from_upstream
            else self._downstream_receive_callback
        )
        try:
            while True:
                data = await source.receive()
                result = callback(source, data)
                if inspect.isawaitable(result):
                    await result
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            if source in source_list:
                source_list.remove(source)

    async def _default_upstream_receive_callback(
        self, source: Connection, data: bytes
    ) -> None:
        """Re-send data to all downstream connections and other upstream connections."""
        targets = [
            *self.downstream_connections,
            *(c for c in self.upstream_connections if c is not source),
        ]
        await self._relay_to(targets, data)

    async def _default_downstream_receive_callback(
        self, source: Connection, data: bytes
    ) -> None:
        """Re-send data to all upstream connections and other downstream connections."""
        targets = [
            *self.upstream_connections,
            *(c for c in self.downstream_connections if c is not source),
        ]
        await self._relay_to(targets, data)

    async def _relay_to(self, targets: List[Connection], data: bytes) -> None:
        for target in targets:
            try:
                await target.send(data)
            except ConnectionError:
                logger.warning("Failed to relay data to %s", target.address)

    async def close(self) -> None:
        """Stop accepting connections and close all tracked connections."""
        for server in self._servers.values():
            server.close()
            await server.wait_closed()
        self._servers.clear()
        self.listening_addresses.clear()

        for connection in (*self.upstream_connections, *self.downstream_connections):
            await connection.close()

        self.upstream_connections.clear()
        self.downstream_connections.clear()

    async def __aenter__(self) -> "BaseLayerNode":
        await self.accept_connection()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

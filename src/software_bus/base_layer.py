from __future__ import annotations

import asyncio
import inspect
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787

Address = Tuple[str, int]

# Messages are framed as a 4-byte big-endian length prefix followed by the
# payload, since TCP gives no message boundaries of its own.
_LENGTH_PREFIX_SIZE = 4

# The most a single message's payload can be (docs/design/data-format.md); the
# same for every implementation, so a node never relays what the next one refuses.
MAX_PAYLOAD_LENGTH = 16 * 1024 * 1024

DEFAULT_SEND_TIMEOUT = 30.0
DEFAULT_SEND_QUEUE_LIMIT = 4 * MAX_PAYLOAD_LENGTH


class PayloadTooLong(ValueError):
    """Data to send is longer than MAX_PAYLOAD_LENGTH. Nothing is sent; not a connection error."""


class ReceivedTooLong(ConnectionError):
    """A peer announced a message longer than MAX_PAYLOAD_LENGTH; the connection has failed."""


class ConnectionTooSlow(ConnectionError):
    """A peer didn't take data within the send timeout, or its send queue would be too full."""


def check_payload_length(length: int) -> None:
    """Raise PayloadTooLong if a payload of `length` bytes can't be sent as one message."""
    if length > MAX_PAYLOAD_LENGTH:
        raise PayloadTooLong(
            f"payload is {length} bytes, over the {MAX_PAYLOAD_LENGTH}-byte limit"
        )


@dataclass
class Connection:
    """A single TCP connection to another instance."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    background_task: Optional[asyncio.Task] = None
    # a node's outgoing data, sent in order by send_task (see BaseLayerNode._relay_to)
    send_queue: Deque[bytes] = field(default_factory=deque)
    queued_bytes: int = 0
    send_task: Optional[asyncio.Task] = None

    @property
    def address(self) -> Optional[Tuple]:
        return self.writer.get_extra_info("peername")

    async def send(self, data: bytes) -> None:
        self.writer.write(len(data).to_bytes(_LENGTH_PREFIX_SIZE, "big") + data)
        await self.writer.drain()

    async def wait_ended(self) -> None:
        """Wait until the connection has failed or been closed (its relay or receive loop has ended)."""
        if self.background_task is not None:
            await asyncio.wait({self.background_task})

    async def receive(self) -> bytes:
        header = await self.reader.readexactly(_LENGTH_PREFIX_SIZE)
        length = int.from_bytes(header, "big")
        if length > MAX_PAYLOAD_LENGTH:
            # not read: skipping it would mean reading it, which the limit is there to prevent
            raise ReceivedTooLong(
                f"peer sent a {length}-byte message, over the {MAX_PAYLOAD_LENGTH}-byte limit"
            )
        return await self.reader.readexactly(length)

    async def close(
        self, *, discard_unsent: bool = False, flush_timeout: Optional[float] = None
    ) -> None:
        """Close the connection.

        With `discard_unsent`, data not sent yet is dropped rather than flushed,
        so closing never waits for a peer that has stopped reading. Otherwise it
        is flushed first, for at most `flush_timeout` seconds if given; what the
        peer hasn't taken by then is dropped.
        """
        unsent = bool(self.send_queue)
        self.send_queue.clear()
        self.queued_bytes = 0
        # don't cancel the task that is closing its own connection
        current = asyncio.current_task()
        for task in (self.background_task, self.send_task):
            if task is not None and task is not current:
                task.cancel()
        if self.writer.is_closing():
            return
        if discard_unsent and (unsent or self.writer.transport.get_write_buffer_size() > 0):
            self.writer.transport.abort()  # close() would first wait to flush it
        else:
            self.writer.close()
        try:
            if flush_timeout is None:
                await self.writer.wait_closed()
                return
            try:
                await asyncio.wait_for(asyncio.shield(self.writer.wait_closed()), flush_timeout)
            except asyncio.TimeoutError:
                self.writer.transport.abort()  # the peer stopped reading: give up on the rest
                await self.writer.wait_closed()
        except ConnectionError:
            pass


ReceiveCallback = Callable[["Connection", bytes], Any]
ConnectionErrorCallback = Callable[["Connection", Optional[BaseException]], Any]
NewConnectionCallback = Callable[["Connection"], Any]


async def _maybe_await(result: Any) -> None:
    """Await `result` if a callback returned an awaitable (i.e. it was async)."""
    if inspect.isawaitable(result):
        await result


async def _call_logging_failure(what: str, callback: Callable[..., Any], *args: Any) -> None:
    """Call a sync or async callback; if it raises, log it and carry on as if it
    had returned (Callbacks in docs/design/base-layer.md)."""
    try:
        await _maybe_await(callback(*args))
    except Exception:
        logger.exception("%s failed", what)


class BaseLayerNode:
    """Base layer of the software bus.

    Accepts TCP connections from downstream instances on any number of
    IP/port pairs and can establish TCP connections to upstream instances,
    tracking both in separate connection lists. Data received on any
    connection is relayed onward per the base layer's routing rule.

    Each connection has its own send queue, so a slow peer can't hold up the
    others; one that is too slow (see `send_timeout`, `send_queue_limit`) is dropped.
    """

    send_timeout = DEFAULT_SEND_TIMEOUT
    send_queue_limit = DEFAULT_SEND_QUEUE_LIMIT

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
        self._upstream_connection_error_callback: ConnectionErrorCallback = (
            self._default_upstream_connection_error_callback
        )
        self._downstream_connection_error_callback: ConnectionErrorCallback = (
            self._default_downstream_connection_error_callback
        )
        self._upstream_new_connection_callback: Optional[NewConnectionCallback] = None
        self._downstream_new_connection_callback: Optional[NewConnectionCallback] = None

    def is_accepting(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> bool:
        return (host, port) in self._servers

    def register_upstream_receive_callback(self, callback: ReceiveCallback) -> None:
        """Set the function to call when data is received from an upstream connection."""
        self._upstream_receive_callback = callback

    def register_downstream_receive_callback(self, callback: ReceiveCallback) -> None:
        """Set the function to call when data is received from a downstream connection."""
        self._downstream_receive_callback = callback

    def register_upstream_connection_error_callback(
        self, callback: ConnectionErrorCallback
    ) -> None:
        """Set the function to call when there is an error with an upstream connection."""
        self._upstream_connection_error_callback = callback

    def register_downstream_connection_error_callback(
        self, callback: ConnectionErrorCallback
    ) -> None:
        """Set the function to call when there is an error with a downstream connection."""
        self._downstream_connection_error_callback = callback

    def register_upstream_new_connection_callback(
        self, callback: Optional[NewConnectionCallback]
    ) -> None:
        """Set the function to call with each newly established upstream connection.

        Pass None to stop calling any function.
        """
        self._upstream_new_connection_callback = callback

    def register_downstream_new_connection_callback(
        self, callback: Optional[NewConnectionCallback]
    ) -> None:
        """Set the function to call with each newly accepted downstream connection.

        Pass None to stop calling any function.
        """
        self._downstream_new_connection_callback = callback

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
        connection = self._start_connection(reader, writer, from_upstream=False)
        logger.info("Accepted downstream connection from %s", connection.address)
        if self._downstream_new_connection_callback is not None:
            await _call_logging_failure(
                "Downstream new connection callback", self._downstream_new_connection_callback, connection
            )

    async def establish_connection(self, host: str, port: int) -> Connection:
        """Open a TCP connection to an upstream instance.

        The resulting connection is appended to `upstream_connections`.
        """
        reader, writer = await asyncio.open_connection(host, port)
        connection = self._start_connection(reader, writer, from_upstream=True)
        logger.info("Established upstream connection to %s:%s", host, port)
        if self._upstream_new_connection_callback is not None:
            await _call_logging_failure(
                "Upstream new connection callback", self._upstream_new_connection_callback, connection
            )
        return connection

    def _start_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        from_upstream: bool,
    ) -> Connection:
        """Track a new connection in its list and start its relay loop."""
        connection = Connection(reader, writer)
        self._connections(from_upstream).append(connection)
        connection.background_task = asyncio.ensure_future(
            self._relay_loop(connection, from_upstream)
        )
        return connection

    def _connections(self, from_upstream: bool) -> List[Connection]:
        return self.upstream_connections if from_upstream else self.downstream_connections

    def _peers_except(self, source: Connection) -> List[Connection]:
        """All downstream and upstream connections other than `source`."""
        return [
            c
            for c in (*self.downstream_connections, *self.upstream_connections)
            if c is not source
        ]

    async def _relay_loop(self, source: Connection, from_upstream: bool) -> None:
        callback = (
            self._upstream_receive_callback
            if from_upstream
            else self._downstream_receive_callback
        )
        error: Optional[BaseException] = None
        cancelled = False
        try:
            while True:
                data = await source.receive()
                try:
                    await _maybe_await(callback(source, data))
                except Exception:
                    # a failing callback costs that one message, not the connection
                    logger.exception("Receive callback failed on data from %s", source.address)
        except (asyncio.IncompleteReadError, OSError) as exc:
            error = exc
        except asyncio.CancelledError:
            cancelled = True  # the connection is being closed on purpose: not an error
        finally:
            if not cancelled:
                await self._handle_connection_error(source, from_upstream, error)

    async def _handle_connection_error(
        self, connection: Connection, from_upstream: bool, error: Optional[BaseException]
    ) -> None:
        source_list = self._connections(from_upstream)
        if connection in source_list:
            source_list.remove(connection)
            error_callback = (
                self._upstream_connection_error_callback
                if from_upstream
                else self._downstream_connection_error_callback
            )
            try:
                await _call_logging_failure("Connection error callback", error_callback, connection, error)
            finally:
                # release the socket; otherwise the server's wait_closed() never returns
                await connection.close(discard_unsent=True)

    async def _default_upstream_receive_callback(
        self, source: Connection, data: bytes
    ) -> None:
        """Re-send data to all downstream connections and other upstream connections."""
        await self._relay_to(self._peers_except(source), data)

    async def _default_downstream_receive_callback(
        self, source: Connection, data: bytes
    ) -> None:
        """Re-send data to all upstream connections and other downstream connections."""
        await self._relay_to(self._peers_except(source), data)

    async def _default_upstream_connection_error_callback(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        """Log that an upstream connection has an error."""
        logger.info(
            "An upstream connection has error: %s (%s)",
            connection.address,
            error,
        )

    async def _default_downstream_connection_error_callback(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        """Log that a downstream connection has an error."""
        logger.info(
            "A downstream connection has error: %s (%s)",
            connection.address,
            error,
        )

    async def _relay_to(self, targets: List[Connection], data: bytes) -> None:
        """Queue `data` on each target, without waiting for it to be sent.

        Data too long to send goes to no target (logged, not a connection error).
        """
        try:
            check_payload_length(len(data))
        except PayloadTooLong as exc:
            logger.error("Not sending: %s", exc)
            return
        for target in targets:
            if target.queued_bytes + len(data) > self.send_queue_limit:
                error = ConnectionTooSlow(
                    f"its send queue would hold over {self.send_queue_limit} bytes"
                )
                from_upstream = target in self.upstream_connections
                await self._handle_connection_error(target, from_upstream, error)
                continue
            target.send_queue.append(data)
            target.queued_bytes += len(data)
            if target.send_task is None or target.send_task.done():
                target.send_task = asyncio.ensure_future(self._send_loop(target))

    async def _send_loop(self, connection: Connection) -> None:
        """Send `connection`'s queue in order; ends once the queue is empty."""
        while connection.send_queue:
            data = connection.send_queue[0]
            try:
                await asyncio.wait_for(connection.send(data), self.send_timeout)
            except asyncio.TimeoutError:
                error: BaseException = ConnectionTooSlow(
                    f"data not taken within {self.send_timeout} seconds"
                )
            except OSError as exc:
                error = exc
            else:
                if connection.writer.is_closing():
                    # closed meanwhile (on Python 3.8, wait_for can swallow that cancellation)
                    return
                connection.send_queue.popleft()
                connection.queued_bytes -= len(data)
                continue
            from_upstream = connection in self.upstream_connections
            await self._handle_connection_error(connection, from_upstream, error)
            return

    async def close_connection(self, connection: Connection) -> None:
        """Close one connection on purpose, keeping the node and its other connections.

        Not an error, so no connection error callback is called. Does nothing if
        `connection` isn't one of this node's (e.g. it failed already). Close a
        node's connections only through here or `close()`: closing a Connection
        itself would leave it in this node's lists.
        """
        for connections in (self.upstream_connections, self.downstream_connections):
            if connection in connections:
                connections.remove(connection)
                await connection.close(discard_unsent=True)  # cancels its relay loop: not reported
                return

    async def close(self) -> None:
        """Stop accepting connections and close all tracked connections."""
        for server in self._servers.values():
            server.close()

        # close connections before wait_closed(), which (Python 3.12.1+) waits
        # for every accepted connection to be closed
        for connection in (*self.upstream_connections, *self.downstream_connections):
            await connection.close(discard_unsent=True)  # doesn't wait for slow peers
        self.upstream_connections.clear()
        self.downstream_connections.clear()

        for server in self._servers.values():
            await server.wait_closed()
        self._servers.clear()
        self.listening_addresses.clear()

    async def __aenter__(self) -> "BaseLayerNode":
        await self.accept_connection()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

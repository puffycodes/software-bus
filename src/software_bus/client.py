from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from .base_layer import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_SEND_TIMEOUT,
    Connection,
    ConnectionErrorCallback,
    ConnectionTooSlow,
    _maybe_await,
    check_payload_length,
)

logger = logging.getLogger(__name__)

ReceiveCallback = Callable[[bytes], Any]


class BaseLayerClient:
    """A client that connects to a Base Layer instance, sends data on that
    connection, and invokes a registered callback with any data received.
    """

    send_timeout = DEFAULT_SEND_TIMEOUT

    def __init__(self) -> None:
        self.connection: Optional[Connection] = None
        self._receive_callback: Optional[ReceiveCallback] = None
        self._connection_error_callback: ConnectionErrorCallback = (
            self._default_connection_error_callback
        )

    async def connect(
        self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
    ) -> Connection:
        """Connect to a node; raises RuntimeError, changing nothing, if already connected.

        To connect again, close() first (or wait for the connection to be lost).
        """
        if self.connection is not None:
            raise RuntimeError("already connected")
        reader, writer = await asyncio.open_connection(host, port)
        connection = Connection(reader, writer)
        if self.connection is not None:
            # another connect() finished first while this one was connecting
            await connection.close()
            raise RuntimeError("already connected")
        connection.background_task = asyncio.ensure_future(
            self._receive_loop(connection)
        )
        self.connection = connection
        logger.info("Connected to %s:%s", host, port)
        return connection

    async def send(self, data: bytes) -> None:
        """Send `data`, waiting until it is handed to the operating system.

        Raises RuntimeError if not connected, or PayloadTooLong if `data` is
        over MAX_PAYLOAD_LENGTH; either way nothing is sent and the connection
        is kept. A send failure, including the node not taking the data within
        `send_timeout` (ConnectionTooSlow), is reported to the connection error
        callback and then re-raised to the caller.
        """
        connection = self.connection
        if connection is None:
            raise RuntimeError("not connected")
        check_payload_length(len(data))
        try:
            try:
                await asyncio.wait_for(connection.send(data), self.send_timeout)
            except asyncio.TimeoutError:
                raise ConnectionTooSlow(
                    f"data not taken within {self.send_timeout} seconds"
                ) from None
        except OSError as exc:
            await self._handle_connection_error(connection, exc)
            raise

    def register_receive_callback(self, callback: Optional[ReceiveCallback]) -> None:
        """Set the function to call with data as it is received.

        Pass None to stop calling any function.
        """
        self._receive_callback = callback

    def register_connection_error_callback(self, callback: ConnectionErrorCallback) -> None:
        """Set the function to call when there is an error with the connection."""
        self._connection_error_callback = callback

    async def _receive_loop(self, connection: Connection) -> None:
        try:
            while True:
                data = await connection.receive()
                await self._on_data_received(data)
        except (asyncio.IncompleteReadError, OSError) as exc:
            await self._handle_connection_error(connection, exc)
        except asyncio.CancelledError:
            pass  # close() cancelled us: not an error

    async def _handle_connection_error(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        """Forget the failed connection, report it, then close it (once only)."""
        if self.connection is not connection:
            return
        self.connection = None
        try:
            await _maybe_await(self._connection_error_callback(connection, error))
        finally:
            await connection.close(discard_unsent=True)

    async def _default_connection_error_callback(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        """Log that the connection has an error."""
        logger.info("The connection has error: %s (%s)", connection.address, error)

    async def _on_data_received(self, data: bytes) -> None:
        if self._receive_callback is None:
            return
        try:
            await _maybe_await(self._receive_callback(data))
        except Exception:
            # a failing callback costs that one message, not the connection
            logger.exception("Receive callback failed")

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def __aenter__(self) -> "BaseLayerClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

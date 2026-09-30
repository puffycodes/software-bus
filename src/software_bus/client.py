from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from .base_layer import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    Connection,
    ConnectionErrorCallback,
    _maybe_await,
)

logger = logging.getLogger(__name__)

ReceiveCallback = Callable[[bytes], Any]


class BaseLayerClient:
    """A client that connects to a Base Layer instance, sends data on that
    connection, and invokes a registered callback with any data received.
    """

    def __init__(self) -> None:
        self.connection: Optional[Connection] = None
        self._receive_callback: Optional[ReceiveCallback] = None
        self._connection_error_callback: ConnectionErrorCallback = (
            self._default_connection_error_callback
        )

    async def connect(
        self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
    ) -> Connection:
        reader, writer = await asyncio.open_connection(host, port)
        connection = Connection(reader, writer)
        connection.background_task = asyncio.ensure_future(
            self._receive_loop(connection)
        )
        self.connection = connection
        logger.info("Connected to %s:%s", host, port)
        return connection

    async def send(self, data: bytes) -> None:
        """Send `data`; a send failure is reported to the connection error
        callback and then re-raised to the caller."""
        connection = self.connection
        if connection is None:
            raise RuntimeError("not connected")
        try:
            await connection.send(data)
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
            await connection.close()

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

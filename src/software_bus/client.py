from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from .base_layer import DEFAULT_HOST, DEFAULT_PORT, Connection, _maybe_await

logger = logging.getLogger(__name__)

ReceiveCallback = Callable[[bytes], Any]


class BaseLayerClient:
    """A client that connects to a Base Layer instance, sends data on that
    connection, and invokes a registered callback with any data received.
    """

    def __init__(self) -> None:
        self.connection: Optional[Connection] = None
        self._receive_callback: Optional[ReceiveCallback] = None

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
        if self.connection is None:
            raise RuntimeError("not connected")
        await self.connection.send(data)

    def register_receive_callback(self, callback: Optional[ReceiveCallback]) -> None:
        """Set the function to call with data as it is received.

        Pass None to stop calling any function.
        """
        self._receive_callback = callback

    async def _receive_loop(self, connection: Connection) -> None:
        try:
            while True:
                data = await connection.receive()
                await self._on_data_received(data)
        except (asyncio.IncompleteReadError, OSError, asyncio.CancelledError):
            pass

    async def _on_data_received(self, data: bytes) -> None:
        if self._receive_callback is not None:
            await _maybe_await(self._receive_callback(data))

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def __aenter__(self) -> "BaseLayerClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

"""Publish/subscribe layer on top of the base layer bus.

Wire format for the payload carried by each base-layer message:

    Subscription: <msg_type=0x01><state (1 byte, 1=subscribe/0=unsubscribe)>
                  <subject_length (2 bytes, big-endian)><subject (utf-8)>
    Publish:      <msg_type=0x02><subject_length (2 bytes, big-endian)>
                  <subject (utf-8)><payload (remaining bytes)>
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .base_layer import DEFAULT_HOST, DEFAULT_PORT, BaseLayerNode, Connection, _maybe_await
from .client import BaseLayerClient

logger = logging.getLogger(__name__)

_MSG_SUBSCRIPTION = 0x01
_MSG_PUBLISH = 0x02
_SUBJECT_LENGTH_SIZE = 2


@dataclass
class SubscriptionMessage:
    """Indicates interest (or loss of interest) in a subject."""

    subject: str
    subscribe: bool


@dataclass
class PublishMessage:
    """A payload published under a subject."""

    subject: str
    payload: bytes


Message = Union[SubscriptionMessage, PublishMessage]

SubscribeCallback = Callable[[str, bool], Any]
PublishCallback = Callable[[str, bytes], Any]


def encode_message(message: Message) -> bytes:
    subject_bytes = message.subject.encode("utf-8")
    subject_header = len(subject_bytes).to_bytes(_SUBJECT_LENGTH_SIZE, "big") + subject_bytes
    if isinstance(message, SubscriptionMessage):
        state = b"\x01" if message.subscribe else b"\x00"
        return bytes([_MSG_SUBSCRIPTION]) + state + subject_header
    if isinstance(message, PublishMessage):
        return bytes([_MSG_PUBLISH]) + subject_header + message.payload
    raise TypeError(f"unsupported message type: {type(message)!r}")


def _decode_subject(data: bytes, offset: int) -> Tuple[str, int]:
    """Decode the length-prefixed subject at `offset`; return it and the offset after it."""
    subject_start = offset + _SUBJECT_LENGTH_SIZE
    subject_length = int.from_bytes(data[offset:subject_start], "big")
    subject_end = subject_start + subject_length
    return data[subject_start:subject_end].decode("utf-8"), subject_end


def decode_message(data: bytes) -> Message:
    msg_type = data[0]
    if msg_type == _MSG_SUBSCRIPTION:
        subscribe = data[1] == 1
        subject, _ = _decode_subject(data, 2)
        return SubscriptionMessage(subject=subject, subscribe=subscribe)
    if msg_type == _MSG_PUBLISH:
        subject, payload_start = _decode_subject(data, 1)
        return PublishMessage(subject=subject, payload=data[payload_start:])
    raise ValueError(f"unknown pub/sub message type: {msg_type!r}")


def _add_subscriber(
    subscriptions: Dict[str, List[Connection]], subject: str, connection: Connection
) -> None:
    connections = subscriptions.setdefault(subject, [])
    if not any(c is connection for c in connections):
        connections.append(connection)


def _discard_subscriber(
    subscriptions: Dict[str, List[Connection]], subject: str, connection: Connection
) -> bool:
    """Remove `connection` from `subject`'s list. Returns True if now empty."""
    connections = subscriptions.get(subject)
    if connections is None:
        return True
    connections[:] = [c for c in connections if c is not connection]
    if not connections:
        subscriptions.pop(subject, None)
        return True
    return False


class PubSubNode:
    """A base layer node that routes subscribe/unsubscribe/publish messages.

    Connection management (`accept_connection`, `establish_connection`,
    `close`) is delegated to an internal `BaseLayerNode`, which is also used
    to send and receive the encoded pub/sub messages.
    """

    def __init__(self) -> None:
        self._base = BaseLayerNode()
        self._base.register_upstream_receive_callback(self._on_upstream_receive)
        self._base.register_downstream_receive_callback(self._on_downstream_receive)
        self._base.register_upstream_connection_error_callback(
            self._on_upstream_connection_error
        )
        self._base.register_downstream_connection_error_callback(
            self._on_downstream_connection_error
        )
        self._downstream_subscriptions: Dict[str, List[Connection]] = {}
        self._upstream_subscriptions: Dict[str, List[Connection]] = {}

    @property
    def upstream_connections(self) -> List[Connection]:
        return self._base.upstream_connections

    @property
    def downstream_connections(self) -> List[Connection]:
        return self._base.downstream_connections

    async def accept_connection(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        return await self._base.accept_connection(host, port)

    async def establish_connection(self, host: str, port: int) -> Connection:
        return await self._base.establish_connection(host, port)

    async def close(self) -> None:
        await self._base.close()
        self._downstream_subscriptions.clear()
        self._upstream_subscriptions.clear()

    async def _on_downstream_receive(self, source: Connection, data: bytes) -> None:
        await self._handle_message(source, decode_message(data), from_upstream=False)

    async def _on_upstream_receive(self, source: Connection, data: bytes) -> None:
        await self._handle_message(source, decode_message(data), from_upstream=True)

    async def _on_downstream_connection_error(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        logger.info("A downstream connection has error: %s (%s)", connection.address, error)
        await self._drop_connection(connection, self._downstream_subscriptions)

    async def _on_upstream_connection_error(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        logger.info("An upstream connection has error: %s (%s)", connection.address, error)
        await self._drop_connection(connection, self._upstream_subscriptions)

    async def _drop_connection(
        self, connection: Connection, subscriptions: Dict[str, List[Connection]]
    ) -> None:
        """Remove a failed connection from every subject and propagate unsubscribes."""
        subjects = [
            subject
            for subject, connections in subscriptions.items()
            if any(c is connection for c in connections)
        ]
        for subject in subjects:
            await self._unsubscribe(subscriptions, subject, connection)

    async def _handle_message(
        self, source: Connection, message: Message, *, from_upstream: bool
    ) -> None:
        if isinstance(message, SubscriptionMessage):
            await self._handle_subscription(source, message, from_upstream=from_upstream)
        elif isinstance(message, PublishMessage):
            await self._handle_publish(message)

    async def _handle_subscription(
        self, source: Connection, message: SubscriptionMessage, *, from_upstream: bool
    ) -> None:
        subject = message.subject
        own_subscriptions = (
            self._upstream_subscriptions if from_upstream else self._downstream_subscriptions
        )
        if message.subscribe:
            _add_subscriber(own_subscriptions, subject, source)
            await self._send_to(self._base._peers_except(source), message)
        else:
            await self._unsubscribe(own_subscriptions, subject, source)

    async def _unsubscribe(
        self,
        subscriptions: Dict[str, List[Connection]],
        subject: str,
        connection: Connection,
    ) -> None:
        became_empty = _discard_subscriber(subscriptions, subject, connection)
        if became_empty and not self._has_subscribers(subject):
            targets = [
                *self._base.downstream_connections,
                *self._base.upstream_connections,
            ]
            await self._send_to(targets, SubscriptionMessage(subject, subscribe=False))

    def _has_subscribers(self, subject: str) -> bool:
        return bool(self._downstream_subscriptions.get(subject)) or bool(
            self._upstream_subscriptions.get(subject)
        )

    async def _handle_publish(self, message: PublishMessage) -> None:
        targets = [
            *self._downstream_subscriptions.get(message.subject, []),
            *self._upstream_subscriptions.get(message.subject, []),
        ]
        await self._send_to(targets, message)

    async def _send_to(self, targets: List[Connection], message: Message) -> None:
        await self._base._relay_to(targets, encode_message(message))

    async def __aenter__(self) -> "PubSubNode":
        await self.accept_connection()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()


class PubSubClient:
    """A leaf client for subscribing to subjects and publishing to them."""

    def __init__(self) -> None:
        self._client = BaseLayerClient()
        self._client.register_receive_callback(self._on_receive)
        self._subscribe_callback: Optional[SubscribeCallback] = None
        self._publish_callback: Optional[PublishCallback] = None

    async def connect(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> Connection:
        return await self._client.connect(host, port)

    async def close(self) -> None:
        await self._client.close()

    def register_subscribe_callback(self, callback: Optional[SubscribeCallback]) -> None:
        """Set the function to call when a subscription message is received.

        Pass None to stop calling any function.
        """
        self._subscribe_callback = callback

    def register_publish_callback(self, callback: Optional[PublishCallback]) -> None:
        """Set the function to call when a publish message is received.

        Pass None to stop calling any function.
        """
        self._publish_callback = callback

    async def subscribe(self, subject: str, subscribe: bool = True) -> None:
        """Subscribe to, or with `subscribe=False`, unsubscribe from a subject."""
        await self._client.send(encode_message(SubscriptionMessage(subject, subscribe)))

    async def publish(self, subject: str, payload: bytes) -> None:
        await self._client.send(encode_message(PublishMessage(subject, payload)))

    async def _on_receive(self, data: bytes) -> None:
        message = decode_message(data)
        if isinstance(message, SubscriptionMessage):
            if self._subscribe_callback is not None:
                await _maybe_await(self._subscribe_callback(message.subject, message.subscribe))
        elif isinstance(message, PublishMessage):
            if self._publish_callback is not None:
                await _maybe_await(self._publish_callback(message.subject, message.payload))

    async def __aenter__(self) -> "PubSubClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

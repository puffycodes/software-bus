"""Publish/subscribe layer on top of the base layer bus.

Wire format for the payload carried by each base-layer message:

    Subscription: <msg_type=0x01><state (1 byte, 1=subscribe/0=unsubscribe)>
                  <subject_length (2 bytes, big-endian)><subject (utf-8)>
    Publish:      <msg_type=0x02><subject_length (2 bytes, big-endian)>
                  <subject (utf-8)><payload (remaining bytes)>
"""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from .base_layer import DEFAULT_HOST, DEFAULT_PORT, BaseLayerNode, Connection
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


def decode_message(data: bytes) -> Message:
    msg_type = data[0]
    if msg_type == _MSG_SUBSCRIPTION:
        subscribe = data[1] == 1
        subject_length = int.from_bytes(data[2 : 2 + _SUBJECT_LENGTH_SIZE], "big")
        subject_start = 2 + _SUBJECT_LENGTH_SIZE
        subject = data[subject_start : subject_start + subject_length].decode("utf-8")
        return SubscriptionMessage(subject=subject, subscribe=subscribe)
    if msg_type == _MSG_PUBLISH:
        subject_length = int.from_bytes(data[1 : 1 + _SUBJECT_LENGTH_SIZE], "big")
        subject_start = 1 + _SUBJECT_LENGTH_SIZE
        subject = data[subject_start : subject_start + subject_length].decode("utf-8")
        payload = data[subject_start + subject_length :]
        return PublishMessage(subject=subject, payload=payload)
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
            if from_upstream:
                targets = [
                    *self._base.downstream_connections,
                    *(c for c in self._base.upstream_connections if c is not source),
                ]
            else:
                targets = [
                    *(c for c in self._base.downstream_connections if c is not source),
                    *self._base.upstream_connections,
                ]
            await self._send_to(targets, message)
        else:
            became_empty = _discard_subscriber(own_subscriptions, subject, source)
            if became_empty and not self._has_subscribers(subject):
                targets = [
                    *self._base.downstream_connections,
                    *self._base.upstream_connections,
                ]
                await self._send_to(targets, message)

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
        data = encode_message(message)
        for target in targets:
            try:
                await target.send(data)
            except ConnectionError:
                logger.warning("Failed to send to %s", target.address)

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
                result = self._subscribe_callback(message.subject, message.subscribe)
                if inspect.isawaitable(result):
                    await result
        elif isinstance(message, PublishMessage):
            if self._publish_callback is not None:
                result = self._publish_callback(message.subject, message.payload)
                if inspect.isawaitable(result):
                    await result

    async def __aenter__(self) -> "PubSubClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

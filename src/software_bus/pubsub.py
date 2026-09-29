"""Publish/subscribe layer on top of the base layer bus.

Wire format for the payload carried by each base-layer message (see
docs/design/data-format.md):

    Subscription:       <msg_type=0x01><state (1 byte, 1=subscribe/0=unsubscribe)>
                        <subject_length (2 bytes, big-endian)><subject (utf-8)>
    Publish:            <msg_type=0x02><subject_length (2 bytes, big-endian)>
                        <subject (utf-8)><payload (remaining bytes)>
    Hello:              <msg_type=0x03><node_id (16 bytes)>
    Reachability Query: <msg_type=0x04><query_id (16 bytes)><target_node_id (16 bytes)>
    Reachability Reply: <msg_type=0x05><query_id (16 bytes)><result (1 byte)>

Hello and the reachability messages are only exchanged between nodes, so a
node can refuse a connection that would close a cycle (see "Cycle Prevention"
in docs/design/publish-subscribe.md).
"""
from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .base_layer import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    BaseLayerNode,
    Connection,
    ConnectionErrorCallback,
    _maybe_await,
)
from .client import BaseLayerClient
from .subject_matcher import StringPatternMatcher

logger = logging.getLogger(__name__)

_MSG_SUBSCRIPTION = 0x01
_MSG_PUBLISH = 0x02
_MSG_HELLO = 0x03
_MSG_REACHABILITY_QUERY = 0x04
_MSG_REACHABILITY_REPLY = 0x05
_SUBJECT_LENGTH_SIZE = 2
_MAX_SUBJECT_LENGTH = 2 ** (8 * _SUBJECT_LENGTH_SIZE) - 1
_ID_SIZE = 16  # Node IDs and query IDs


class ReachabilityResult(IntEnum):
    NOT_FOUND = 0
    FOUND = 1
    UNKNOWN = 2


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


@dataclass
class HelloMessage:
    """Sent by a node on every new connection, identifying it by its Node ID."""

    node_id: bytes


@dataclass
class ReachabilityQueryMessage:
    """Asks whether the node `target_node_id` can be reached through the receiver."""

    query_id: bytes
    target_node_id: bytes


@dataclass
class ReachabilityReplyMessage:
    """The answer to the reachability query `query_id`."""

    query_id: bytes
    result: ReachabilityResult


Message = Union[
    SubscriptionMessage,
    PublishMessage,
    HelloMessage,
    ReachabilityQueryMessage,
    ReachabilityReplyMessage,
]

PublishCallback = Callable[[str, str, bytes], Any]
"""Called with (matched_subject, actual_subject, payload)."""


class CycleCheckRefused(ConnectionError):
    """Raised by `PubSubNode.establish_connection` when the cycle check refuses
    the new connection; the message says why (e.g. it would create a cycle)."""


def _check_id(value: bytes, name: str) -> bytes:
    if len(value) != _ID_SIZE:
        raise ValueError(f"{name} must be {_ID_SIZE} bytes, not {len(value)}")
    return bytes(value)


def encode_message(message: Message) -> bytes:
    if isinstance(message, HelloMessage):
        return bytes([_MSG_HELLO]) + _check_id(message.node_id, "node_id")
    if isinstance(message, ReachabilityQueryMessage):
        return (
            bytes([_MSG_REACHABILITY_QUERY])
            + _check_id(message.query_id, "query_id")
            + _check_id(message.target_node_id, "target_node_id")
        )
    if isinstance(message, ReachabilityReplyMessage):
        return (
            bytes([_MSG_REACHABILITY_REPLY])
            + _check_id(message.query_id, "query_id")
            + bytes([ReachabilityResult(message.result)])
        )
    if not isinstance(message, (SubscriptionMessage, PublishMessage)):
        raise TypeError(f"unsupported message type: {type(message)!r}")
    subject_bytes = message.subject.encode("utf-8")
    if len(subject_bytes) > _MAX_SUBJECT_LENGTH:
        raise ValueError(
            f"subject is {len(subject_bytes)} bytes, over the {_MAX_SUBJECT_LENGTH}-byte limit"
        )
    subject_header = len(subject_bytes).to_bytes(_SUBJECT_LENGTH_SIZE, "big") + subject_bytes
    if isinstance(message, SubscriptionMessage):
        state = b"\x01" if message.subscribe else b"\x00"
        return bytes([_MSG_SUBSCRIPTION]) + state + subject_header
    return bytes([_MSG_PUBLISH]) + subject_header + message.payload


def _decode_subject(data: bytes, offset: int) -> Tuple[str, int]:
    """Decode the length-prefixed subject at `offset`; return it and the offset after it."""
    subject_start = offset + _SUBJECT_LENGTH_SIZE
    if len(data) < subject_start:
        raise ValueError("pub/sub message too short for its subject length")
    subject_length = int.from_bytes(data[offset:subject_start], "big")
    subject_end = subject_start + subject_length
    if len(data) < subject_end:
        raise ValueError("pub/sub message shorter than its subject length")
    # UnicodeDecodeError is a ValueError, so a bad subject is reported the same way
    return data[subject_start:subject_end].decode("utf-8"), subject_end


def _expect_length(data: bytes, length: int, name: str) -> None:
    if len(data) != length:
        raise ValueError(f"{name} message must be {length} bytes, not {len(data)}")


def decode_message(data: bytes) -> Message:
    """Decode a pub/sub payload; raises ValueError if it is malformed."""
    if not data:
        raise ValueError("empty pub/sub message")
    msg_type = data[0]
    if msg_type == _MSG_SUBSCRIPTION:
        if len(data) < 2 or data[1] not in (0, 1):
            raise ValueError("invalid subscription state")
        subject, _ = _decode_subject(data, 2)
        return SubscriptionMessage(subject=subject, subscribe=data[1] == 1)
    if msg_type == _MSG_PUBLISH:
        subject, payload_start = _decode_subject(data, 1)
        return PublishMessage(subject=subject, payload=data[payload_start:])
    if msg_type == _MSG_HELLO:
        _expect_length(data, 1 + _ID_SIZE, "hello")
        return HelloMessage(node_id=bytes(data[1:]))
    if msg_type == _MSG_REACHABILITY_QUERY:
        _expect_length(data, 1 + 2 * _ID_SIZE, "reachability query")
        return ReachabilityQueryMessage(
            query_id=bytes(data[1 : 1 + _ID_SIZE]), target_node_id=bytes(data[1 + _ID_SIZE :])
        )
    if msg_type == _MSG_REACHABILITY_REPLY:
        _expect_length(data, 1 + _ID_SIZE + 1, "reachability reply")
        try:
            result = ReachabilityResult(data[-1])
        except ValueError:
            raise ValueError(f"invalid reachability result: {data[-1]!r}") from None
        return ReachabilityReplyMessage(query_id=bytes(data[1 : 1 + _ID_SIZE]), result=result)
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
    """Remove `connection` from `subject`'s list. Returns True if it was there."""
    connections = subscriptions.get(subject)
    if connections is None or not any(c is connection for c in connections):
        return False
    connections[:] = [c for c in connections if c is not connection]
    if not connections:
        subscriptions.pop(subject, None)
    return True


def _matching_connections(
    subscriptions: Dict[str, List[Connection]], subject: str, matcher: StringPatternMatcher
) -> List[Connection]:
    """Every connection tagged to a subscribed subject that `subject` matches.

    A connection subscribed under more than one tag that matches `subject`
    (e.g. both "a.b" and "a.*") is still only returned once, so it receives
    a single wire copy of the publish rather than one per matching tag.
    """
    connections: List[Connection] = []
    seen_ids = set()
    for tagged_subject, tagged_connections in subscriptions.items():
        if not matcher.match(subject, tagged_subject):
            continue
        for connection in tagged_connections:
            if id(connection) not in seen_ids:
                seen_ids.add(id(connection))
                connections.append(connection)
    return connections


@dataclass
class _PendingConnection:
    """An established connection that the cycle check has not accepted yet."""

    hello: asyncio.Future  # resolves to the peer's Node ID
    held: List[Message] = field(default_factory=list)  # subscriptions/publishes, in order


class _Query:
    """A reachability query sent to `targets`, combining their replies."""

    def __init__(self, targets: List[Connection]) -> None:
        self.targets = targets
        self._waiting = {id(c) for c in targets}
        self._saw_unknown = False
        self.result: asyncio.Future = asyncio.get_running_loop().create_future()

    def reply(self, connection: Connection, result: ReachabilityResult) -> None:
        """Record `connection`'s reply (a failed connection counts as not found)."""
        if self.result.done() or id(connection) not in self._waiting:
            return
        self._waiting.discard(id(connection))
        if result == ReachabilityResult.FOUND:
            self.result.set_result(ReachabilityResult.FOUND)
            return
        if result == ReachabilityResult.UNKNOWN:
            self._saw_unknown = True
        if not self._waiting:
            self.result.set_result(
                ReachabilityResult.UNKNOWN if self._saw_unknown else ReachabilityResult.NOT_FOUND
            )


class PubSubNode:
    """A base layer node that routes subscribe/unsubscribe/publish messages.

    Connection management (`accept_connection`, `establish_connection`,
    `close`) is delegated to an internal `BaseLayerNode`, which is also used
    to send and receive the encoded pub/sub messages.

    `establish_connection` refuses (raising `CycleCheckRefused`) a connection
    that would close a cycle; see "Cycle Prevention" in
    docs/design/publish-subscribe.md. The class attributes below are that
    check's timings.
    """

    check_timeout: float = 5.0
    max_check_attempts: int = 5
    check_retry_delay: Tuple[float, float] = (0.2, 2.0)

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
        self._base.register_upstream_new_connection_callback(self._on_upstream_new_connection)
        self._base.register_downstream_new_connection_callback(
            self._on_downstream_new_connection
        )
        self._downstream_subscriptions: Dict[str, List[Connection]] = {}
        self._upstream_subscriptions: Dict[str, List[Connection]] = {}
        self._matcher = StringPatternMatcher()

        self.node_id: bytes = uuid.uuid4().bytes
        # keyed by id(connection): Connection is an unhashable dataclass
        self._peer_node_ids: Dict[int, bytes] = {}  # node connections -> their Node ID
        self._pending: Dict[int, _PendingConnection] = {}
        self._draining: Dict[int, List[Message]] = {}  # accepted, held messages not yet processed
        self._queries: Dict[bytes, _Query] = {}  # query ID -> query being answered
        self._query_tasks: Set[asyncio.Task] = set()
        self._checking = False  # an own reachability query is waiting for replies
        self._check_lock: Optional[asyncio.Lock] = None  # created lazily, inside the event loop

    @property
    def upstream_connections(self) -> List[Connection]:
        return self._base.upstream_connections

    @property
    def downstream_connections(self) -> List[Connection]:
        return self._base.downstream_connections

    async def accept_connection(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        return await self._base.accept_connection(host, port)

    async def establish_connection(self, host: str, port: int) -> Connection:
        """Connect to an upstream node and run the cycle check on the connection.

        Returns once the connection is accepted. Raises `CycleCheckRefused`
        (a `ConnectionError`) saying why if it is refused, or another
        `ConnectionError` if the connection is lost during the check.
        """
        connection = await self._base.establish_connection(host, port)
        await self._check_new_connection(connection)
        return connection

    async def close(self) -> None:
        for task in list(self._query_tasks):
            task.cancel()
        await self._base.close()
        self._downstream_subscriptions.clear()
        self._upstream_subscriptions.clear()
        self._peer_node_ids.clear()
        self._draining.clear()
        self._queries.clear()

    # --- connection lifecycle ---------------------------------------------

    async def _on_upstream_new_connection(self, connection: Connection) -> None:
        # pending before the first await, so nothing received on it is processed as active
        self._pending[id(connection)] = _PendingConnection(
            hello=asyncio.get_running_loop().create_future()
        )
        await self._send_to([connection], HelloMessage(self.node_id))

    async def _on_downstream_new_connection(self, connection: Connection) -> None:
        await self._send_to([connection], HelloMessage(self.node_id))
        await self._send_subscriptions(connection)

    async def _send_subscriptions(self, connection: Connection) -> None:
        """Tell a newly joined connection about every subject that has subscribers."""
        subjects = {*self._downstream_subscriptions, *self._upstream_subscriptions}
        for subject in subjects:
            await self._send_to([connection], SubscriptionMessage(subject, subscribe=True))

    async def _on_downstream_connection_error(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        logger.info("A downstream connection has error: %s (%s)", connection.address, error)
        self._forget_connection(connection)
        await self._drop_connection(connection, self._downstream_subscriptions)

    async def _on_upstream_connection_error(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        logger.info("An upstream connection has error: %s (%s)", connection.address, error)
        self._forget_connection(connection)
        await self._drop_connection(connection, self._upstream_subscriptions)

    def _forget_connection(self, connection: Connection) -> None:
        """Drop the cycle-prevention state held for a failed connection."""
        self._peer_node_ids.pop(id(connection), None)
        self._draining.pop(id(connection), None)
        pending = self._pending.pop(id(connection), None)
        if pending is not None and not pending.hello.done():
            pending.hello.set_exception(
                ConnectionError("connection closed before the peer identified itself")
            )
            pending.hello.exception()  # retrieved: no "never retrieved" log if nobody waits
        for query in list(self._queries.values()):
            query.reply(connection, ReachabilityResult.NOT_FOUND)

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

    def _active(self, connections: List[Connection]) -> List[Connection]:
        """`connections` without pending ones, which are never sent to."""
        return [c for c in connections if id(c) not in self._pending]

    def _active_node_connections(self) -> List[Connection]:
        return [
            c
            for c in self._active(
                [*self._base.downstream_connections, *self._base.upstream_connections]
            )
            if id(c) in self._peer_node_ids
        ]

    # --- cycle check on an established connection ---------------------------

    async def _check_new_connection(self, connection: Connection) -> None:
        if self._check_lock is None:
            self._check_lock = asyncio.Lock()
        try:
            # one check at a time: two checked together could each pass and still form a cycle
            async with self._check_lock:
                reason = await self._run_check(connection)
                if reason is None:
                    await self._activate(connection)
                    return
        except BaseException:
            # lost during the check, or cancelled: don't leave it half set up
            await self._base._handle_connection_error(connection, True, None)
            raise
        # info, not error: the caller gets the reason in the exception (ps_server prints it)
        logger.info("Refused upstream connection to %s: %s", connection.address, reason)
        error = CycleCheckRefused(f"refused by the cycle check: {reason}")
        await self._base._handle_connection_error(connection, True, error)
        raise error

    async def _run_check(self, connection: Connection) -> Optional[str]:
        """Return None to accept `connection`, or the reason to refuse it."""
        pending = self._still_pending(connection)
        try:
            peer_id = await asyncio.wait_for(asyncio.shield(pending.hello), self.check_timeout)
        except asyncio.TimeoutError:
            return "the peer did not identify itself as a publish/subscribe node"
        if peer_id == self.node_id:
            return "it is a connection to this node itself"
        if any(self._peer_node_ids.get(id(c)) == peer_id for c in self._active_node_connections()):
            return "this node is already connected to that node"

        for attempt in range(self.max_check_attempts):
            if attempt:
                await asyncio.sleep(random.uniform(*self.check_retry_delay))
                self._still_pending(connection)
            result = await self._ask_own_query(peer_id)
            self._still_pending(connection)
            if result == ReachabilityResult.FOUND:
                return "it would create a cycle"
            if result == ReachabilityResult.NOT_FOUND:
                return None
        return "could not determine whether it would create a cycle"

    def _still_pending(self, connection: Connection) -> _PendingConnection:
        pending = self._pending.get(id(connection))
        if pending is None:
            raise ConnectionError("connection closed during the cycle check")
        return pending

    async def _ask_own_query(self, target_node_id: bytes) -> ReachabilityResult:
        targets = self._active_node_connections()
        if not targets:
            return ReachabilityResult.NOT_FOUND
        query_id = uuid.uuid4().bytes
        self._checking = True
        try:
            return await self._collect(query_id, self._open_query(query_id, targets), target_node_id)
        finally:
            self._checking = False

    async def _activate(self, connection: Connection) -> None:
        """Accept a checked connection: process what it sent meanwhile, in order."""
        held = self._pending.pop(id(connection)).held
        # anything it sends while we drain queues up behind the held messages
        self._draining[id(connection)] = held
        try:
            await self._send_subscriptions(connection)
            # stop if it fails meanwhile: its messages must not tag a dead connection
            while held and id(connection) in self._draining:
                await self._handle_message(connection, held.pop(0), from_upstream=True)
        finally:
            self._draining.pop(id(connection), None)

    # --- reachability queries -----------------------------------------------

    def _open_query(self, query_id: bytes, targets: List[Connection]) -> _Query:
        query = _Query(targets)
        self._queries[query_id] = query
        return query

    async def _collect(
        self, query_id: bytes, query: _Query, target_node_id: bytes
    ) -> ReachabilityResult:
        """Send `query` to its targets and return their combined replies."""
        try:
            await self._send_to(query.targets, ReachabilityQueryMessage(query_id, target_node_id))
            try:
                return await asyncio.wait_for(asyncio.shield(query.result), self.check_timeout)
            except asyncio.TimeoutError:
                return ReachabilityResult.UNKNOWN
        finally:
            if self._queries.get(query_id) is query:
                del self._queries[query_id]

    def _answer_now(
        self, source: Connection, message: ReachabilityQueryMessage
    ) -> Optional[ReachabilityResult]:
        """The reply to a query that needs no forwarding, or None to forward it."""
        if id(source) in self._pending:
            # a pending connection isn't there yet, even to reach this node itself
            return ReachabilityResult.UNKNOWN if self._checking else ReachabilityResult.NOT_FOUND
        if message.target_node_id == self.node_id:
            return ReachabilityResult.FOUND
        if self._checking:
            # a check that depends on our pending connection must retry after ours
            return ReachabilityResult.UNKNOWN
        if message.query_id in self._queries:
            logger.warning(
                "Reachability query %s arrived twice: there is already a cycle",
                message.query_id.hex(),
            )
            return ReachabilityResult.UNKNOWN
        return None

    async def _handle_query(self, source: Connection, message: ReachabilityQueryMessage) -> None:
        result = self._answer_now(source, message)
        if result is None:
            targets = [c for c in self._active_node_connections() if c is not source]
            if targets:
                # registered now, so a duplicate arriving meanwhile is spotted;
                # replies are awaited in a task so `source`'s relay loop isn't blocked
                query = self._open_query(message.query_id, targets)
                task = asyncio.ensure_future(self._forward_query(source, message, query))
                self._query_tasks.add(task)
                task.add_done_callback(self._query_tasks.discard)
                return
            result = ReachabilityResult.NOT_FOUND
        await self._send_to([source], ReachabilityReplyMessage(message.query_id, result))

    async def _forward_query(
        self, source: Connection, message: ReachabilityQueryMessage, query: _Query
    ) -> None:
        result = await self._collect(message.query_id, query, message.target_node_id)
        if any(c is source for c in (*self.upstream_connections, *self.downstream_connections)):
            await self._send_to([source], ReachabilityReplyMessage(message.query_id, result))

    # --- message processing -------------------------------------------------

    async def _on_downstream_receive(self, source: Connection, data: bytes) -> None:
        await self._on_receive(source, data, from_upstream=False)

    async def _on_upstream_receive(self, source: Connection, data: bytes) -> None:
        await self._on_receive(source, data, from_upstream=True)

    async def _on_receive(self, source: Connection, data: bytes, *, from_upstream: bool) -> None:
        try:
            message = decode_message(data)
        except ValueError as exc:
            logger.warning("Ignoring malformed message from %s: %s", source.address, exc)
            return
        if isinstance(message, (SubscriptionMessage, PublishMessage)):
            held = self._held_messages(source)
            if held is not None:
                held.append(message)
                return
        await self._handle_message(source, message, from_upstream=from_upstream)

    def _held_messages(self, source: Connection) -> Optional[List[Message]]:
        """Where to keep `source`'s messages until it is accepted, or None if it is."""
        pending = self._pending.get(id(source))
        if pending is not None:
            return pending.held
        return self._draining.get(id(source))

    async def _handle_message(
        self, source: Connection, message: Message, *, from_upstream: bool
    ) -> None:
        if isinstance(message, SubscriptionMessage):
            await self._handle_subscription(source, message, from_upstream=from_upstream)
        elif isinstance(message, PublishMessage):
            await self._handle_publish(source, message)
        elif isinstance(message, HelloMessage):
            self._peer_node_ids[id(source)] = message.node_id
            pending = self._pending.get(id(source))
            if pending is not None and not pending.hello.done():
                pending.hello.set_result(message.node_id)
        elif isinstance(message, ReachabilityQueryMessage):
            await self._handle_query(source, message)
        elif isinstance(message, ReachabilityReplyMessage):
            query = self._queries.get(message.query_id)
            if query is not None:
                query.reply(source, message.result)

    async def _handle_subscription(
        self, source: Connection, message: SubscriptionMessage, *, from_upstream: bool
    ) -> None:
        subject = message.subject
        own_subscriptions = (
            self._upstream_subscriptions if from_upstream else self._downstream_subscriptions
        )
        if message.subscribe:
            _add_subscriber(own_subscriptions, subject, source)
            await self._send_to(self._active(self._base._peers_except(source)), message)
        else:
            await self._unsubscribe(own_subscriptions, subject, source)

    async def _unsubscribe(
        self,
        subscriptions: Dict[str, List[Connection]],
        subject: str,
        connection: Connection,
    ) -> None:
        """Remove `connection` from `subject` and tell each neighbour that this
        node no longer wants the subject from it, when that is now true.

        A neighbour N is still wanted-from while any connection other than N
        subscribes, so: no subscribers left -> tell everyone but `connection`;
        exactly one left -> tell only that one; more -> tell nobody.
        """
        if not _discard_subscriber(subscriptions, subject, connection):
            return
        remaining = self._subscribers(subject)
        if not remaining:
            targets = [
                c
                for c in self._active(
                    [*self._base.downstream_connections, *self._base.upstream_connections]
                )
                if c is not connection
            ]
        elif len(remaining) == 1:
            targets = remaining
        else:
            return
        await self._send_to(targets, SubscriptionMessage(subject, subscribe=False))

    def _subscribers(self, subject: str) -> List[Connection]:
        return [
            *self._downstream_subscriptions.get(subject, []),
            *self._upstream_subscriptions.get(subject, []),
        ]

    async def _handle_publish(self, source: Connection, message: PublishMessage) -> None:
        # never send a publish back where it came from: two neighbouring nodes
        # with subscribers would otherwise bounce it between them forever
        targets = [
            c
            for c in (
                *_matching_connections(
                    self._downstream_subscriptions, message.subject, self._matcher
                ),
                *_matching_connections(
                    self._upstream_subscriptions, message.subject, self._matcher
                ),
            )
            if c is not source
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
        self._subscribe_callbacks: Dict[str, List[PublishCallback]] = {}
        self._matcher = StringPatternMatcher()

    async def connect(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> Connection:
        return await self._client.connect(host, port)

    async def close(self) -> None:
        await self._client.close()

    def register_connection_error_callback(self, callback: ConnectionErrorCallback) -> None:
        """Set the function to call when there is an error with the connection upstream."""
        self._client.register_connection_error_callback(callback)

    async def subscribe(self, subject: str, callback: PublishCallback) -> None:
        """Subscribe to `subject`, calling `callback` for every matching publish.

        `callback` is called with (matched_subject, actual_subject, payload).
        A subject can have multiple callbacks registered against it; the
        upstream node is only told about the subscription once, for the
        first callback registered against a given subject.
        """
        # encode first: a subject too long to send must not leave a callback registered
        wire_message = encode_message(SubscriptionMessage(subject, subscribe=True))
        callbacks = self._subscribe_callbacks.setdefault(subject, [])
        is_first_subscription = not callbacks
        callbacks.append(callback)
        if is_first_subscription:
            await self._client.send(wire_message)

    async def unsubscribe(self, subject: str, callback: PublishCallback) -> None:
        """Remove `callback` from `subject`.

        The upstream node is only told about the unsubscription once no
        callback remains registered against `subject`.
        """
        callbacks = self._subscribe_callbacks.get(subject)
        if callbacks is None:
            return
        callbacks[:] = [c for c in callbacks if c is not callback]
        if not callbacks:
            self._subscribe_callbacks.pop(subject, None)
            await self._client.send(encode_message(SubscriptionMessage(subject, subscribe=False)))

    async def publish(self, subject: str, payload: bytes) -> None:
        """Publish `payload` under `subject`.

        The node never sends a publish back to its sender, so this client's
        own matching subscription callbacks are called here directly.
        """
        message = PublishMessage(subject, payload)
        await self._client.send(encode_message(message))
        await self._deliver(message)

    async def _on_receive(self, data: bytes) -> None:
        try:
            message = decode_message(data)
        except ValueError as exc:
            logger.warning("Ignoring malformed message: %s", exc)
            return
        if isinstance(message, SubscriptionMessage):
            logger.info(
                "Received subscription message: subject=%r subscribe=%s",
                message.subject,
                message.subscribe,
            )
        elif isinstance(message, PublishMessage):
            await self._deliver(message)
        elif isinstance(message, HelloMessage):
            pass  # every node greets a new connection; clients have no use for it
        else:
            logger.info("Ignoring %s: only nodes take part in cycle checks", type(message).__name__)

    async def _deliver(self, message: PublishMessage) -> None:
        """Call every callback whose subscribed subject matches the published one."""
        # snapshot: a callback may subscribe/unsubscribe while we iterate
        for subscribed_subject, callbacks in list(self._subscribe_callbacks.items()):
            if self._matcher.match(message.subject, subscribed_subject):
                for callback in list(callbacks):
                    await _maybe_await(
                        callback(subscribed_subject, message.subject, message.payload)
                    )

    async def __aenter__(self) -> "PubSubClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

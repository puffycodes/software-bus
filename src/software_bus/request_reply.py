"""Request/reply on top of the publish/subscribe layer (docs/design/request-reply.md).

A request is a publish to `_request.<request_subject>`; its payload is a
Request Message (see docs/design/data-format.md):

    <reply_subject_length (2 bytes, big-endian)><reply_subject (utf-8)>
    <request_payload (remaining bytes)>

The requestor subscribes to its reply subject, `_reply.<request_subject>.<unique_id>`,
before publishing, and every acceptor publishes its reply there as-is.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import string
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple

from .base_layer import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    Connection,
    ConnectionErrorCallback,
    _call_logging_failure,
    _maybe_await,
)
from .pubsub import (
    _MAX_SUBJECT_LENGTH,
    _SUBJECT_LENGTH_SIZE,
    PublishCallback,
    PubSubClient,
    _decode_subject,
    _encode_subject,
    check_publish_length,
)
from .subject_matcher import has_wildcard

logger = logging.getLogger(__name__)

REQUEST_PREFIX = "_request"
REPLY_PREFIX = "_reply"
UNIQUE_ID_LENGTH = 32
_UNIQUE_ID_ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase
DEFAULT_TIMEOUT = 5 * 60.0

ReceivedRequestCallback = Callable[[str, str, bytes], Any]
"""Called with (request_subject, reply_subject, request_payload)."""



class ReplyStatus(Enum):
    """Why a received reply callback is called: with a reply, or because the wait ended without one."""

    REPLY = "reply"
    TIMED_OUT = "timed out"
    CLOSED = "closed"
    CONNECTION_LOST = "connection lost"


ReceivedReplyCallback = Callable[[str, bytes, ReplyStatus], Any]
"""Called with (reply_subject, reply_payload, status); a true return value ends the wait
(ignored unless status is REPLY)."""


@dataclass
class RequestMessage:
    """The payload of a publish to a Derived Request Subject."""

    reply_subject: str
    payload: bytes


def encode_request(message: RequestMessage) -> bytes:
    return _encode_subject(message.reply_subject, "reply subject") + message.payload


def decode_request(data: bytes) -> RequestMessage:
    """Decode a Request Message; raises ValueError if it is malformed."""
    reply_subject, payload_start = _decode_subject(data, 0)
    return RequestMessage(reply_subject=reply_subject, payload=data[payload_start:])


def derived_request_subject(request_subject: str) -> str:
    return f"{REQUEST_PREFIX}.{request_subject}"


def derived_reply_subject(request_subject: str, unique_id: str) -> str:
    return f"{REPLY_PREFIX}.{request_subject}.{unique_id}"


def new_unique_id() -> str:
    """A random, unguessable unique_id for a Derived Reply Subject."""
    return "".join(secrets.choice(_UNIQUE_ID_ALPHABET) for _ in range(UNIQUE_ID_LENGTH))


def _is_unique_id(value: str) -> bool:
    return len(value) == UNIQUE_ID_LENGTH and all(c in _UNIQUE_ID_ALPHABET for c in value)


def is_valid_reply_subject(subject: str) -> bool:
    """True if `subject` has the form of a Derived Reply Subject."""
    parts = subject.split(".")
    return (
        len(parts) >= 3
        and parts[0] == REPLY_PREFIX
        and _is_unique_id(parts[-1])
        and not has_wildcard(subject)
    )


def _is_reply_subject_for(reply_subject: str, request_subject: str) -> bool:
    """True if `reply_subject` is a Derived Reply Subject of `request_subject`."""
    prefix = f"{REPLY_PREFIX}.{request_subject}."
    return (
        is_valid_reply_subject(reply_subject)
        and reply_subject.startswith(prefix)
        and _is_unique_id(reply_subject[len(prefix) :])
    )


def _check_request_subject(request_subject: str, longest_derived: str) -> None:
    """Parameter Checks for a request subject, given the longest derived subject it is used in."""
    if has_wildcard(request_subject):
        raise ValueError(f"request subject {request_subject!r} contains a wild card")
    length = len(longest_derived.encode("utf-8"))
    if length > _MAX_SUBJECT_LENGTH:
        raise ValueError(
            f"derived subject is {length} bytes, over the {_MAX_SUBJECT_LENGTH}-byte limit"
        )


def check_request_subject(request_subject: str, sending: bool = False) -> None:
    """Raise ValueError if `request_subject` fails the Parameter Checks for
    accepting requests on it or, with `sending`, for sending a request to it."""
    if sending:
        longest = derived_reply_subject(request_subject, "0" * UNIQUE_ID_LENGTH)
    else:
        longest = derived_request_subject(request_subject)
    _check_request_subject(request_subject, longest)


@dataclass
class _PendingRequest:
    """A sent request whose replies are being waited for."""

    reply_subject: str
    callback: ReceivedReplyCallback
    subscription: Optional[PublishCallback] = None
    timer: Optional["asyncio.Future[None]"] = None
    sending: bool = True  # Send Request hasn't returned yet
    # set when close/a connection error skipped it while sending: how to end it once sent
    closed_meanwhile: Optional[ReplyStatus] = None
    ended: bool = False


class RequestReplyClient:
    """A leaf client that accepts requests, sends them, and replies to them."""

    def __init__(self) -> None:
        self._client = PubSubClient()
        self._client.register_connection_error_callback(self._on_connection_error)
        self._connection_error_callback: ConnectionErrorCallback = (
            self._default_connection_error_callback
        )
        self._acceptors: Dict[Tuple[str, ReceivedRequestCallback], PublishCallback] = {}
        self._requests: Dict[str, _PendingRequest] = {}

    async def connect(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> Connection:
        """Connect to a node, as `PubSubClient.connect` does.

        Acceptors belong to the client, not to a connection: their subscriptions
        are sent again here, so they keep accepting requests after reconnecting.
        Pending requests don't carry over: `close` and a lost connection end them.
        """
        return await self._client.connect(host, port)

    def register_connection_error_callback(self, callback: ConnectionErrorCallback) -> None:
        """Set the function to call when there is an error with the connection upstream.

        Pending requests are ended (as by `close`) before it is called.
        """
        self._connection_error_callback = callback

    async def close(self) -> None:
        """End every pending request's wait (reported as CLOSED), then close the connection."""
        await self._end_all_without_reply(ReplyStatus.CLOSED)
        await self._client.close()

    async def accept_request(self, request_subject: str, callback: ReceivedRequestCallback) -> None:
        """Call `callback(request_subject, reply_subject, request_payload)` for every request
        sent to `request_subject`."""
        subject = derived_request_subject(request_subject)
        _check_request_subject(request_subject, subject)
        key = (request_subject, callback)
        if key in self._acceptors:
            return  # once per request, not once per registration

        async def on_request(matched: str, actual: str, payload: bytes) -> None:
            try:
                message = decode_request(payload)
            except ValueError as exc:
                logger.warning("Ignoring malformed request to %r: %s", request_subject, exc)
                return
            if not _is_reply_subject_for(message.reply_subject, request_subject):
                logger.warning(
                    "Ignoring request to %r with invalid reply subject %r",
                    request_subject,
                    message.reply_subject,
                )
                return
            try:
                await _maybe_await(callback(request_subject, message.reply_subject, message.payload))
            except Exception:
                logger.exception("Received request callback for %r failed", request_subject)

        self._acceptors[key] = on_request
        try:
            await self._client.subscribe(subject, on_request)
        except BaseException:
            del self._acceptors[key]  # subscribe has removed its callback itself
            raise

    async def stop_accept_request(
        self, request_subject: str, callback: ReceivedRequestCallback
    ) -> None:
        """Stop calling `callback` for requests sent to `request_subject`."""
        subject = derived_request_subject(request_subject)
        _check_request_subject(request_subject, subject)
        on_request = self._acceptors.pop((request_subject, callback), None)
        if on_request is not None:
            await self._client.unsubscribe(subject, on_request)

    async def send_request(
        self,
        request_subject: str,
        payload: bytes,
        callback: ReceivedReplyCallback,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> str:
        """Send a request and call `callback` with every reply until the wait ends.

        Returns the request's Derived Reply Subject, which identifies it to
        `cancel_request`. A timeout of 0 means wait forever.
        """
        reply_subject = derived_reply_subject(request_subject, new_unique_id())
        _check_request_subject(request_subject, reply_subject)
        if timeout < 0:
            raise ValueError(f"timeout must be >= 0, not {timeout!r}")
        request_message_length = (
            _SUBJECT_LENGTH_SIZE + len(reply_subject.encode("utf-8")) + len(payload)
        )
        check_publish_length(derived_request_subject(request_subject), request_message_length)

        request = _PendingRequest(reply_subject, callback)

        async def on_reply(matched: str, actual: str, reply_payload: bytes) -> None:
            if request.ended:
                return
            try:
                done = await _maybe_await(callback(actual, reply_payload, ReplyStatus.REPLY))
            except Exception:
                # counts as a false return value: keep waiting
                logger.exception("Received reply callback for %r failed", reply_subject)
                return
            if done and not request.ended:
                await self._end_wait(request)

        request.subscription = on_reply
        self._requests[reply_subject] = request
        try:
            await self._client.subscribe(reply_subject, on_reply)
            # started before publishing: our own acceptors can reply during the publish
            if timeout > 0:
                request.timer = asyncio.ensure_future(self._run_timer(request, timeout))
            await self._client.publish(
                derived_request_subject(request_subject),
                encode_request(RequestMessage(reply_subject, payload)),
            )
        except BaseException:
            if not request.ended:
                await self._end_wait(request)
            raise
        finally:
            request.sending = False
        if request.closed_meanwhile is not None and not request.ended:
            # sent, but the client was closed meanwhile: end it as close/the error would have
            await self._end_without_reply(request, request.closed_meanwhile)
        return reply_subject

    async def cancel_request(self, reply_subject: str) -> None:
        """End the wait for the request `reply_subject` without calling its callback."""
        request = self._requests.get(reply_subject)
        if request is not None and not request.ended:
            await self._end_wait(request)

    async def send_reply(self, reply_subject: str, payload: bytes) -> None:
        """Publish `payload` as a reply to the requestor listening on `reply_subject`."""
        if not is_valid_reply_subject(reply_subject):
            raise ValueError(f"{reply_subject!r} is not a valid reply subject")
        check_publish_length(reply_subject, len(payload))
        await self._client.publish(reply_subject, payload)

    async def _run_timer(self, request: _PendingRequest, timeout: float) -> None:
        await asyncio.sleep(timeout)
        request.timer = None  # fired: ending the wait mustn't cancel this task
        if not request.ended:
            await self._end_without_reply(request, ReplyStatus.TIMED_OUT)

    async def _end_wait(self, request: _PendingRequest) -> None:
        """End of the wait: done exactly once per request."""
        request.ended = True
        self._requests.pop(request.reply_subject, None)
        if request.timer is not None:
            request.timer.cancel()
            request.timer = None
        if request.subscription is not None:
            await self._unsubscribe_quietly(request.reply_subject, request.subscription)

    async def _end_without_reply(self, request: _PendingRequest, status: ReplyStatus) -> None:
        """End the wait and make the single call saying why, with no reply."""
        await self._end_wait(request)
        try:
            await _maybe_await(request.callback(request.reply_subject, b"", status))
        except Exception:
            logger.exception("Received reply callback for %r failed", request.reply_subject)

    async def _end_all_without_reply(self, status: ReplyStatus) -> None:
        # a request still being sent reports its failure through send_request instead
        for request in list(self._requests.values()):
            if request.ended:
                continue
            if request.sending:
                request.closed_meanwhile = status  # send_request ends it once sent
            else:
                await self._end_without_reply(request, status)

    async def _unsubscribe_quietly(self, subject: str, callback: PublishCallback) -> None:
        """Unsubscribe; if the node can't be told, the subscription is still removed locally."""
        try:
            await self._client.unsubscribe(subject, callback)
        except (OSError, RuntimeError) as exc:
            logger.debug("Could not tell the node about unsubscribing %r: %s", subject, exc)

    async def _on_connection_error(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        await self._end_all_without_reply(ReplyStatus.CONNECTION_LOST)
        await _call_logging_failure(
            "Connection error callback",
            self._connection_error_callback,
            connection,
            error,
            log=logger,
        )

    async def _default_connection_error_callback(
        self, connection: Connection, error: Optional[BaseException]
    ) -> None:
        """Log that the connection has an error, as the pub/sub client does by default."""
        logger.info("The connection has error: %s (%s)", connection.address, error)

    async def __aenter__(self) -> "RequestReplyClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

import asyncio
import logging

import pytest

from software_bus import PubSubClient, PubSubNode, ReplyStatus, RequestMessage, RequestReplyClient
from software_bus.base_layer import MAX_PAYLOAD_LENGTH, PayloadTooLong
from software_bus.request_reply import (
    decode_request,
    derived_reply_subject,
    derived_request_subject,
    encode_request,
    is_valid_reply_subject,
    new_unique_id,
)

ID = "A" * 32
REPLY = ReplyStatus.REPLY
TIMED_OUT = ReplyStatus.TIMED_OUT
CLOSED = ReplyStatus.CLOSED
CONNECTION_LOST = ReplyStatus.CONNECTION_LOST


async def start_node():
    node = PubSubNode()
    server = await node.accept_connection(port=0)
    return node, server.sockets[0].getsockname()[1]


async def connected_client(port):
    client = RequestReplyClient()
    await client.connect("127.0.0.1", port)
    return client


def subscribed_subjects(client):
    return set(client._client._subscribe_callbacks)


def test_request_message_wire_layout():
    message = RequestMessage(f"_reply.a.{ID}", b"\x00hi")
    reply_subject = f"_reply.a.{ID}".encode()
    assert encode_request(message) == b"\x00" + bytes([len(reply_subject)]) + reply_subject + b"\x00hi"
    assert decode_request(encode_request(message)) == message


@pytest.mark.parametrize("payload", [b"", bytes(range(256))])
def test_request_message_roundtrip(payload):
    message = RequestMessage(f"_reply.données.{ID}", payload)
    assert decode_request(encode_request(message)) == message


@pytest.mark.parametrize("data", [b"", b"\x00", b"\x00\x05ab", b"\x00\x01\xff"])
def test_decode_malformed_request_raises(data):
    with pytest.raises(ValueError):
        decode_request(data)


def test_encode_request_rejects_reply_subject_over_length_limit():
    with pytest.raises(ValueError):
        encode_request(RequestMessage("x" * 65536, b""))


def test_derived_subjects_and_unique_id():
    assert derived_request_subject("a.b") == "_request.a.b"
    assert derived_reply_subject("a.b", ID) == f"_reply.a.b.{ID}"
    first, second = new_unique_id(), new_unique_id()
    assert len(first) == 32 and first.isalnum() and first.isascii()
    assert first != second


@pytest.mark.parametrize(
    "subject, valid",
    [
        (f"_reply.a.{ID}", True),
        (f"_reply.a.b.{ID}", True),
        (f"_reply.{ID}", False),  # nothing between _reply and the unique_id
        (f"_request.a.{ID}", False),
        ("_reply.a." + "A" * 31, False),
        ("_reply.a." + "A" * 31 + "_", False),
        (f"_reply.*.{ID}", False),
        ("orders.new", False),
    ],
)
def test_valid_reply_subject(subject, valid):
    assert is_valid_reply_subject(subject) is valid


@pytest.mark.asyncio
async def test_parameter_checks_reject_and_change_nothing():
    client = RequestReplyClient()  # not connected: a check that passed would fail on sending
    callback = lambda *args: None
    with pytest.raises(ValueError):
        await client.accept_request("a.*", callback)
    with pytest.raises(ValueError):
        await client.stop_accept_request("*", callback)
    with pytest.raises(ValueError):
        await client.send_request("a.*", b"", callback)
    with pytest.raises(ValueError):
        await client.send_request("a", b"", callback, timeout=-1)
    with pytest.raises(ValueError):
        await client.send_reply("orders.new", b"")
    # longest allowed: 65535 minus "_request." (9 bytes), or minus "_reply." + "." + id (40 bytes)
    with pytest.raises(ValueError):
        await client.accept_request("x" * 65527, callback)
    with pytest.raises(ValueError):
        await client.send_request("x" * 65496, b"", callback)
    assert subscribed_subjects(client) == set()
    assert client._acceptors == {} and client._requests == {}


@pytest.mark.asyncio
async def test_send_reply_when_not_connected_raises():
    client = RequestReplyClient()
    with pytest.raises(RuntimeError):
        await client.send_reply(f"_reply.a.{ID}", b"x")


@pytest.mark.asyncio
async def test_subjects_at_the_length_limits_are_accepted():
    node, port = await start_node()
    client = await connected_client(port)
    try:
        await client.accept_request("x" * 65526, lambda *args: None)
        await client.send_request("x" * 65495, b"", lambda *args: None, timeout=0)
    finally:
        await client.close()
        await node.close()


# Request to "a": the publish is tag + 2 + "_request.a" (10) + 2 + "_reply.a.<id>" (41) + payload.
# A reply to "_reply.a.<id>" is tag + 2 + 41 + payload.
LONGEST_REQUEST_PAYLOAD = MAX_PAYLOAD_LENGTH - 56
LONGEST_REPLY_PAYLOAD = MAX_PAYLOAD_LENGTH - 44


@pytest.mark.asyncio
async def test_payloads_over_the_size_limit_are_rejected_and_change_nothing():
    client = RequestReplyClient()  # not connected: a check that passed would fail on sending
    with pytest.raises(PayloadTooLong):
        await client.send_request("a", b"x" * (LONGEST_REQUEST_PAYLOAD + 1), lambda *args: None)
    with pytest.raises(PayloadTooLong):
        await client.send_reply(f"_reply.a.{ID}", b"x" * (LONGEST_REPLY_PAYLOAD + 1))
    assert subscribed_subjects(client) == set()
    assert client._requests == {}


@pytest.mark.asyncio
async def test_payloads_at_the_size_limit_are_accepted():
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    requests = []
    reply = asyncio.get_event_loop().create_future()
    try:
        async def on_request(request_subject, reply_subject, payload):
            requests.append(len(payload))
            await replier.send_reply(reply_subject, b"y" * LONGEST_REPLY_PAYLOAD)

        await replier.accept_request("a", on_request)
        await asyncio.sleep(0.05)

        def on_reply(reply_subject, payload, status):
            reply.set_result((len(payload), status))
            return True

        await requester.send_request("a", b"x" * LONGEST_REQUEST_PAYLOAD, on_reply, timeout=10)

        assert await asyncio.wait_for(reply, timeout=10) == (LONGEST_REPLY_PAYLOAD, REPLY)
        assert requests == [LONGEST_REQUEST_PAYLOAD]
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_request_reaches_acceptor_and_reply_ends_the_wait():
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    requests, replies = [], []
    try:
        async def on_request(request_subject, reply_subject, payload):
            requests.append((request_subject, reply_subject, payload))
            await replier.send_reply(reply_subject, payload.upper())

        await replier.accept_request("svc.echo", on_request)
        await asyncio.sleep(0.05)

        def on_reply(reply_subject, payload, status):
            replies.append((reply_subject, payload, status))
            return True

        handle = await requester.send_request("svc.echo", b"hi", on_reply, timeout=0.3)
        await asyncio.sleep(0.5)  # past the timeout: no TIMED_OUT call may follow

        assert handle.startswith("_reply.svc.echo.")
        assert requests == [("svc.echo", handle, b"hi")]
        assert replies == [(handle, b"HI", REPLY)]
        assert handle not in subscribed_subjects(requester)
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_one_callback_accepting_several_subjects_gets_each_request_subject():
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    requests = []
    try:
        def on_request(request_subject, reply_subject, payload):
            requests.append((request_subject, reply_subject, payload))

        await replier.accept_request("svc.a", on_request)
        await replier.accept_request("svc.b", on_request)
        await asyncio.sleep(0.05)

        first = await requester.send_request("svc.a", b"1", lambda *args: None, timeout=0)
        second = await requester.send_request("svc.b", b"2", lambda *args: None, timeout=0)
        await asyncio.sleep(0.1)

        assert requests == [("svc.a", first, b"1"), ("svc.b", second, b"2")]
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_every_acceptor_replies_and_the_wait_lasts_until_the_timeout():
    node, port = await start_node()
    repliers = [await connected_client(port) for _ in range(2)]
    requester = await connected_client(port)
    replies = []
    try:
        for index, replier in enumerate(repliers):
            async def on_request(request_subject, reply_subject, payload, replier=replier, index=index):
                await replier.send_reply(reply_subject, b"r%d" % index)

            await replier.accept_request("svc", on_request)
        await asyncio.sleep(0.05)

        handle = await requester.send_request(
            "svc", b"", lambda *args: replies.append(args), timeout=0.3
        )
        await asyncio.sleep(0.5)

        assert set(replies[:2]) == {(handle, b"r0", REPLY), (handle, b"r1", REPLY)}
        assert replies[2:] == [(handle, b"", TIMED_OUT)]
        assert handle not in subscribed_subjects(requester)
    finally:
        await requester.close()
        for replier in repliers:
            await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_client_receives_its_own_request_and_reply_before_send_request_returns():
    node, port = await start_node()
    client = await connected_client(port)
    replies = []
    try:
        async def on_request(request_subject, reply_subject, payload):
            await client.send_reply(reply_subject, b"own")

        await client.accept_request("svc", on_request)

        def on_reply(reply_subject, payload, status):
            replies.append((payload, status))
            return True

        await client.send_request("svc", b"", on_reply, timeout=0.2)
        assert replies == [(b"own", REPLY)]
        # the timer started before the publish, so ending the wait cancelled it
        await asyncio.sleep(0)  # let the cancelled timer finish
        assert not [
            task for task in asyncio.all_tasks()
            if not task.done() and task.get_coro().__qualname__.endswith("_run_timer")
        ]
        await asyncio.sleep(0.4)  # the timer was cancelled, though it started before the publish
        assert replies == [(b"own", REPLY)]
    finally:
        await client.close()
        await node.close()


@pytest.mark.asyncio
async def test_timeout_without_a_reply():
    node, port = await start_node()
    requester = await connected_client(port)
    replies = []
    try:
        handle = await requester.send_request(
            "nobody", b"x", lambda *args: replies.append(args), timeout=0.1
        )
        await asyncio.sleep(0.3)
        assert replies == [(handle, b"", TIMED_OUT)]
        assert subscribed_subjects(requester) == set()
    finally:
        await requester.close()
        await node.close()


@pytest.mark.asyncio
async def test_timeout_zero_waits_until_cancelled_and_cancel_calls_nothing():
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    reply_subjects, replies = [], []
    try:
        await replier.accept_request("svc", lambda request_subject, reply_subject, payload: reply_subjects.append(reply_subject))
        await asyncio.sleep(0.05)
        handle = await requester.send_request("svc", b"", lambda *args: replies.append(args), timeout=0)
        await asyncio.sleep(0.2)
        assert handle in subscribed_subjects(requester)

        await requester.cancel_request(handle)
        await requester.cancel_request(handle)  # already ended: nothing happens
        await requester.cancel_request(f"_reply.unknown.{ID}")
        assert handle not in subscribed_subjects(requester)

        await replier.send_reply(reply_subjects[0], b"late")
        await asyncio.sleep(0.1)
        assert replies == []
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_close_ends_pending_requests_as_closed():
    node, port = await start_node()
    requester = await connected_client(port)
    replies = []
    try:
        first = await requester.send_request("a", b"", lambda *args: replies.append(args), timeout=0)
        second = await requester.send_request("b", b"", lambda *args: replies.append(args))
        await requester.close()
        assert set(replies) == {(first, b"", CLOSED), (second, b"", CLOSED)}
        assert requester._requests == {}
    finally:
        await requester.close()
        await node.close()


@pytest.mark.asyncio
async def test_connection_error_ends_pending_requests_then_calls_the_callback():
    node, port = await start_node()
    requester = await connected_client(port)
    events = []
    try:
        requester.register_connection_error_callback(lambda connection, error: events.append("error"))
        handle = await requester.send_request(
            "a", b"", lambda *args: events.append(args), timeout=0
        )
        await node.close()
        await asyncio.sleep(0.2)
        assert events == [(handle, b"", CONNECTION_LOST), "error"]
        assert requester._requests == {}
    finally:
        await requester.close()
        await node.close()


@pytest.mark.asyncio
async def test_a_failing_connection_error_callback_is_logged_as_request_reply(caplog):
    node, port = await start_node()
    requester = await connected_client(port)
    replies = []
    try:
        def failing_callback(connection, error):
            raise RuntimeError("callback failed")

        requester.register_connection_error_callback(failing_callback)
        handle = await requester.send_request("a", b"", lambda *args: replies.append(args), timeout=0)
        with caplog.at_level(logging.ERROR, logger="software_bus.request_reply"):
            await node.close()
            await asyncio.sleep(0.2)
        # pending requests are still ended, and the failure is logged under this layer
        assert replies == [(handle, b"", CONNECTION_LOST)]
        assert any(
            r.name == "software_bus.request_reply" and "Connection error callback failed" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await requester.close()
        await node.close()


@pytest.mark.asyncio
async def test_acceptor_keeps_accepting_after_the_client_reconnects():
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    replies = []
    try:
        async def on_request(request_subject, reply_subject, payload):
            await replier.send_reply(reply_subject, b"pong")

        await replier.accept_request("svc", on_request)
        await replier.close()
        await replier.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)

        def on_reply(reply_subject, payload, status):
            replies.append((payload, status))
            return True

        await requester.send_request("svc", b"ping", on_reply, timeout=1)
        await asyncio.sleep(0.1)
        assert replies == [(b"pong", REPLY)]
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_send_request_when_not_connected_raises_and_leaves_nothing():
    client = RequestReplyClient()
    replies = []
    with pytest.raises(RuntimeError):
        await client.send_request("a", b"", lambda *args: replies.append(args), timeout=0.1)
    await asyncio.sleep(0.2)
    assert replies == []
    assert client._requests == {}
    assert subscribed_subjects(client) == set()


@pytest.mark.asyncio
async def test_connection_lost_while_sending_is_reported_once_as_an_error():
    """The failure is raised from send_request; the connection error doesn't
    also end this request's wait with a timed-out call."""
    node, port = await start_node()
    requester = await connected_client(port)
    replies, errors = [], []
    try:
        requester.register_connection_error_callback(lambda connection, error: errors.append(error))
        pending = await requester.send_request("a", b"", lambda *args: replies.append(args), timeout=0)

        async def publish_losing_the_connection(subject, payload):
            # what BaseLayerClient.send does when sending fails
            error = OSError("connection lost")
            await requester._on_connection_error(requester._client._client.connection, error)
            raise error

        requester._client.publish = publish_losing_the_connection
        with pytest.raises(OSError):
            await requester.send_request("b", b"", lambda *args: replies.append(args), timeout=0)

        assert replies == [(pending, b"", CONNECTION_LOST)]  # only the request that had been sent
        assert len(errors) == 1
        assert requester._requests == {}
        assert subscribed_subjects(requester) == set()
    finally:
        await requester.close()
        await node.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "closed_by, status", [("close", CLOSED), ("connection error", CONNECTION_LOST)]
)
async def test_request_sent_while_the_client_is_closed_ends_once_sent(closed_by, status):
    """Close (or a connection error) skips a request still being sent; if the
    send then succeeds, send_request ends the wait itself, as close would have."""
    node, port = await start_node()
    requester = await connected_client(port)
    replies = []
    try:
        publish = requester._client.publish

        async def publish_then_close(subject, payload):
            await publish(subject, payload)
            if closed_by == "close":
                await requester.close()
            else:
                await requester._on_connection_error(requester._client._client.connection, None)

        requester._client.publish = publish_then_close
        handle = await requester.send_request(
            "a", b"", lambda *args: replies.append(args), timeout=0
        )
        assert replies == [(handle, b"", status)]
        assert requester._requests == {}
        assert subscribed_subjects(requester) == set()
    finally:
        await requester.close()
        await node.close()


@pytest.mark.asyncio
async def test_accept_request_when_not_connected_raises_and_leaves_nothing():
    client = RequestReplyClient()
    with pytest.raises(RuntimeError):
        await client.accept_request("svc", lambda *args: None)
    assert client._acceptors == {}
    assert subscribed_subjects(client) == set()


@pytest.mark.asyncio
async def test_stop_accept_request_after_connection_lost_raises_but_forgets_the_acceptor():
    node, port = await start_node()
    replier = await connected_client(port)
    callback = lambda *args: None
    try:
        await replier.accept_request("svc", callback)
        await node.close()
        await asyncio.sleep(0.1)  # the client notices the connection is gone
        with pytest.raises(RuntimeError):
            await replier.stop_accept_request("svc", callback)
        assert replier._acceptors == {}
        assert subscribed_subjects(replier) == set()
    finally:
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_failing_reply_callback_keeps_the_wait(caplog):
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    replies = []
    try:
        async def on_request(request_subject, reply_subject, payload):
            await replier.send_reply(reply_subject, b"1")
            await replier.send_reply(reply_subject, b"2")

        await replier.accept_request("svc", on_request)
        await asyncio.sleep(0.05)

        def on_reply(reply_subject, payload, status):
            replies.append(payload)
            if payload == b"1":
                raise RuntimeError("boom")
            return True

        with caplog.at_level(logging.ERROR, logger="software_bus.request_reply"):
            await requester.send_request("svc", b"", on_reply, timeout=1)
            await asyncio.sleep(0.2)
        assert replies == [b"1", b"2"]
        assert "boom" in caplog.text
        assert requester._requests == {}
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_timeout_during_a_running_reply_callback_ends_the_wait_once():
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    events = []
    try:
        await replier.accept_request(
            "svc",
            lambda request_subject, reply_subject, payload: asyncio.ensure_future(
                replier.send_reply(reply_subject, b"r")
            ),
        )
        await asyncio.sleep(0.05)

        async def on_reply(reply_subject, payload, status):
            events.append(("start", status))
            if status is REPLY:
                await asyncio.sleep(0.3)  # the timeout fires meanwhile
                events.append("returns true")
            return True

        await requester.send_request("svc", b"", on_reply, timeout=0.1)
        await asyncio.sleep(0.6)
        assert events == [("start", REPLY), ("start", TIMED_OUT), "returns true"]
        assert requester._requests == {}
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_accept_and_stop_accept_per_callback(caplog):
    node, port = await start_node()
    replier = await connected_client(port)
    requester = await connected_client(port)
    first, second = [], []

    def on_first(request_subject, reply_subject, payload):
        first.append(payload)
        raise RuntimeError("first failed")

    def on_second(request_subject, reply_subject, payload):
        second.append(payload)

    try:
        await replier.accept_request("svc", on_first)
        await replier.accept_request("svc", on_first)  # already registered: still once per request
        await replier.accept_request("svc", on_second)
        await asyncio.sleep(0.05)

        with caplog.at_level(logging.ERROR, logger="software_bus.request_reply"):
            await requester.send_request("svc", b"1", lambda *args: None, timeout=0)
            await asyncio.sleep(0.1)
        assert first == [b"1"] and second == [b"1"]
        assert "Received request callback for 'svc' failed" in caplog.text

        await replier.stop_accept_request("svc", on_first)
        await replier.stop_accept_request("svc", on_first)  # not registered any more: nothing happens
        await requester.send_request("svc", b"2", lambda *args: None, timeout=0)
        await asyncio.sleep(0.1)
        assert first == [b"1"] and second == [b"1", b"2"]

        await replier.stop_accept_request("svc", on_second)
        await asyncio.sleep(0.05)
        assert subscribed_subjects(replier) == set()
    finally:
        await requester.close()
        await replier.close()
        await node.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        b"\x00",  # malformed: cut-off reply subject length
        encode_request(RequestMessage("orders.new", b"x")),  # not a reply subject
        encode_request(RequestMessage(f"_reply.other.{ID}", b"x")),  # for another request subject
    ],
)
async def test_acceptor_ignores_bad_requests(payload, caplog):
    node, port = await start_node()
    replier = await connected_client(port)
    publisher = PubSubClient()
    received = []
    try:
        await replier.accept_request("svc", lambda *args: received.append(args))
        await publisher.connect("127.0.0.1", port)
        await asyncio.sleep(0.05)
        with caplog.at_level(logging.WARNING, logger="software_bus.request_reply"):
            await publisher.publish("_request.svc", payload)
            await asyncio.sleep(0.1)
        assert received == []
        assert "Ignoring" in caplog.text
    finally:
        await publisher.close()
        await replier.close()
        await node.close()

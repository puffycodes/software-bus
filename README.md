# software-bus

A layered software bus for connecting distributed instances over TCP.

Instances connect to each other as **upstream** (toward the hub/root) and
**downstream** (branching out) peers, forming a tree. Data is flooded to
every other connection: anything received from a downstream connection is
relayed to all upstream connections and to every other downstream
connection; anything received from an upstream connection is relayed to
all downstream connections and to every other upstream connection.

See [`docs/design/base-layer.md`](docs/design/base-layer.md) for the full
design and routing rules, and
[`docs/design/publish-subscribe.md`](docs/design/publish-subscribe.md) for
the publish/subscribe layer built on top of it, including how it matches a
published subject against a subscribed one (see
[`docs/design/subject-matcher.md`](docs/design/subject-matcher.md)), and
[`docs/design/request-reply.md`](docs/design/request-reply.md) for the
request/reply client built on publish/subscribe. The
wire formats for all the layers are specified in
[`docs/design/data-format.md`](docs/design/data-format.md), and the
`bus_check` cycle-finding tool in
[`docs/design/bus-check.md`](docs/design/bus-check.md).

**The bus trusts every peer.** There's no authentication, encryption or
access control: any process that can connect to a node can publish,
subscribe (to anything, including other clients' requests and replies) or
act as a node. Run it on one machine (the default, 127.0.0.1) or on a
private network where every machine and user is trusted. See "Trust Model"
in [`docs/design/base-layer.md`](docs/design/base-layer.md) for what this
leaves out of scope.

## Install

```
python3 -m pip install --user -e ".[dev]"
```

The project has no `setup.py`, so an editable install needs pip 21.3 or
newer (pip fetches the `setuptools>=64` it needs by itself). Older systems
ship an older pip — Ubuntu 20.04's is 20.0.2 — and fail with an error about
a missing `setup.py`. You don't need a newer Python to fix that: upgrade pip
for your user only (pip 25.0.x is the last release that supports
Python 3.8), then install as above:

```
python3 -m pip install --user --upgrade "pip<25.1"
```

On Windows, `python3` is often only a Microsoft Store placeholder, so use
`python` instead.

## Running the tests

```
python3 -m pytest -v
```

[`docs/test-cases.md`](docs/test-cases.md) describes every test in plain
English, including the manual tests that have to be run by hand.
[`docs/development-approach.md`](docs/development-approach.md) describes
how the project is developed and tested, and
[`docs/development-guideline.md`](docs/development-guideline.md) is a
condensed, project-neutral version of it for reuse in other projects.

Without installing, you can run code and tests with `src/` on the path
instead:

```
PYTHONPATH=src python3 -m pytest -v            # POSIX shells, e.g. bash or Git Bash
$env:PYTHONPATH = "src"; python -m pytest -v   # PowerShell
```

## Usage

### `BaseLayerNode` — a bus node

Accepts downstream connections on one or more IP/port pairs, can connect
out to an upstream instance, and relays data per the routing rule above.
Relaying is driven by an upstream and a downstream receive callback, each
called with the source `Connection` and the received data; the routing
rule above is just the default behavior of those callbacks, and either can
be replaced with `register_upstream_receive_callback` /
`register_downstream_receive_callback`.

```python
import asyncio
from software_bus import BaseLayerNode

async def main():
    hub = BaseLayerNode()
    await hub.accept_connection("127.0.0.1", 8787)  # listen for downstream peers
    await hub.establish_connection("10.0.0.1", 8787)  # connect to an upstream peer

    # ... run for a while ...

    await hub.close()

asyncio.run(main())
```

Override a callback to change how received data is handled, instead of
the default relay behavior:

```python
hub.register_downstream_receive_callback(
    lambda source, data: print("from downstream:", data)
)
```

If a receive callback raises an exception, the node logs it with its
traceback and carries on with the next message: the connection is kept, and
this doesn't count as a connection error. The same goes for every callback
you register: a failing new-connection callback keeps the connection (and
`establish_connection` still returns it), and a failing connection error
callback doesn't stop the connection being removed and closed.

When a connection drops, or a send/receive on it fails, it's removed from
`upstream_connections`/`downstream_connections` and an upstream or
downstream connection error callback is called with the `Connection` and
the exception — e.g. `asyncio.IncompleteReadError` when the peer closed the
connection. Once the callback returns, the connection is closed, so don't
keep it around to send on. Closing the node yourself with `close()` doesn't
call these callbacks (the peers at the other end do see their connections
drop), and neither does `close_connection(connection)`, which closes one
connection on purpose and keeps the rest. Close a node's connections only
through those two: calling `close()` on a `Connection` itself leaves it in
the node's lists until a send to it fails. On a `PubSubNode`,
`close_connection` also unsubscribes the connection from its subjects, so
the rest of the tree stops routing to it. The default callbacks just log the event; override them with `register_upstream_connection_error_callback`
/ `register_downstream_connection_error_callback` to react instead:

```python
hub.register_downstream_connection_error_callback(
    lambda connection, error: print("downstream peer gone:", connection.address, error)
)
```

To react when a connection opens, register a function with
`register_upstream_new_connection_callback` /
`register_downstream_new_connection_callback`; it's called with the new
`Connection`. None is registered by default.

Each connection has its own send queue, so relaying to a peer that has
stopped reading doesn't hold up the others. A peer that's too slow is
dropped like any other failed connection (the error is a
`ConnectionTooSlow`): either it doesn't take queued data within
`send_timeout` (30 seconds), or its queue would hold more than
`send_queue_limit` (64 MiB). Both are attributes you can change on a node.
Closing a node, or dropping a failed connection, discards data not sent yet
rather than waiting for a slow peer.

#### Wire format

See [`docs/design/data-format.md`](docs/design/data-format.md) for the
canonical spec this section summarizes.

TCP gives no message boundaries of its own, so every connection (whether
it carries raw `BaseLayerClient`/`bl_client` data or an encoded pub/sub
message) is framed by `Connection` the same way — a 4-byte big-endian
length prefix followed by exactly that many bytes of payload:

| Field    | Bytes             |
|----------|-------------------|
| `length` | 4 (big-endian)    |
| `data`   | `length` bytes    |

There's no message-type byte at this layer; `data` is opaque to
`BaseLayerNode`/`BaseLayerClient` and is relayed as-is. It's up to
whatever's built on top — such as the pub/sub layer below — to give that
payload further structure.

`length` is at most `MAX_PAYLOAD_LENGTH`, 16 MiB, for every node and
client. Sending more raises `PayloadTooLong` (a `ValueError`) and sends
nothing. Receiving a larger `length` drops the connection
(`ReceivedTooLong`) without reading the payload.

### `BaseLayerClient` — a leaf client

A plain point-to-point client for talking to a `BaseLayerNode` node: connect,
send, and register a callback for incoming data. A `BaseLayerNode` never
echoes data back to the connection it came from, so the example below
uses two clients — `sender` won't see its own message.

```python
import asyncio
from software_bus import BaseLayerClient

async def main():
    sender = BaseLayerClient()
    await sender.connect("127.0.0.1", 8787)

    receiver = BaseLayerClient()
    receiver.register_receive_callback(lambda data: print("received:", data))
    await receiver.connect("127.0.0.1", 8787)

    await sender.send(b"hello")

    # ... run for a while ...

    await sender.close()
    await receiver.close()

asyncio.run(main())
```

If the connection drops, or a send/receive on it fails, the client forgets
it (`client.connection` becomes `None`), calls its connection error callback
with the `Connection` and the exception, and closes it. A failed `send` also
re-raises the error to the caller. `send` waits until the operating system
has taken the data; if that takes longer than the client's `send_timeout`
(30 seconds), the send fails with `ConnectionTooSlow`. Data over
`MAX_PAYLOAD_LENGTH` raises `PayloadTooLong` without touching the
connection. The default callback just logs; register
your own to react, e.g. to reconnect:

```python
client.register_connection_error_callback(
    lambda connection, error: print("lost connection:", connection.address, error)
)
```

Closing the client yourself with `close()` doesn't call the callback. Data
already sent is still delivered, but `close()` waits at most `close_timeout`
(5 seconds) for the node to take it, so it never hangs on a node that has
stopped reading.

A client holds one connection at a time: `connect()` on a client that's
already connected raises `RuntimeError` and keeps the existing connection,
and `send()` on a client that isn't connected raises `RuntimeError` too.
To reconnect, `close()` first, or connect again once the connection has
been lost. The same goes for `PubSubClient` and `RequestReplyClient`.
Their subscriptions (and request acceptors) belong to the client, not the
connection: they survive `close()` or a lost connection, and `connect()`
subscribes the node it connects to to all of them again, so reconnecting
resumes them. Pending requests don't carry over: they end with
`ReplyStatus.CLOSED` or `ReplyStatus.CONNECTION_LOST`.

As on a node, an exception raised by the receive callback is logged and
costs only that one message: the client stays connected and keeps
receiving. A connection error callback that raises is logged too, and a
failed `send` still raises its own error, not the callback's.

### `PubSubNode` and `PubSubClient` — publish/subscribe

A publish/subscribe layer built on top of `BaseLayerNode`/`BaseLayerClient`.
A **subject** is a `.`-separated string (e.g. `"a.b"`) of at most 65535
bytes once UTF-8 encoded; a longer one raises `ValueError`. A `PubSubClient`
subscribes to a subject with a callback to receive future publishes tagged
with it, and publishes payloads under a subject; a `PubSubNode` tracks, per
subscribed subject, which of its connections are interested and only
forwards a publish to those, propagating subscribe/unsubscribe through the
tree as needed.

A subject subscribed to (whether by a `PubSubClient` or received as a
subscription message by a `PubSubNode`) may be a literal subject or a
wildcard pattern, e.g. `"a.*"` matches any published subject with two
`.`-separated parts whose first part is `a`. A published subject itself is
always literal: `publish` raises `ValueError` for a subject with a `*` part
(e.g. `"a.*"`), sending nothing. Matching is done by `StringPatternMatcher`
(`software_bus.subject_matcher`) — see
[`docs/design/subject-matcher.md`](docs/design/subject-matcher.md) for the
full matching rules.

```python
import asyncio
from software_bus import PubSubClient, PubSubNode

async def main():
    node = PubSubNode()
    await node.accept_connection("127.0.0.1", 8787)

    subscriber = PubSubClient()
    await subscriber.connect("127.0.0.1", 8787)
    await subscriber.subscribe(
        "a.b", lambda matched, actual, payload: print(f"{matched} {actual}: {payload!r}")
    )

    publisher = PubSubClient()
    await publisher.connect("127.0.0.1", 8787)
    await publisher.publish("a.b", b"hello")

    # ... run for a while ...

    await subscriber.close()
    await publisher.close()
    await node.close()

asyncio.run(main())
```

The callback is called with `(matched_subject, actual_subject, payload)` —
`matched_subject` is the subject you subscribed with (literal or pattern),
`actual_subject` is the literal subject the publisher used; a
subject can have multiple callbacks registered against it, and the upstream
node is only told about the subscription once, for the first callback
registered on a given subject. Subscribing the same function to the same
subject twice registers it twice, so it's called twice per publish. If the
subscription can't be sent (e.g. the client isn't connected), `subscribe`
raises and leaves nothing registered. If an unsubscribe can't be sent, the
callback is still removed and the error raised; if a publish can't be
sent, the error is raised and the client's own callbacks aren't called.
`unsubscribe(subject, callback)` removes one registration of that callback
(so a function subscribed twice needs two unsubscribes), only telling the
upstream node once no callback remains for that subject — so keep a
reference to the function you subscribed with:

```python
def on_publish(matched, actual, payload):
    print(f"{matched} {actual}: {payload!r}")

await subscriber.subscribe("a.b", on_publish)
# ... later ...
await subscriber.unsubscribe("a.b", on_publish)
```

A client that is subscribed to a subject it publishes to receives its own
publish: `publish()` calls the client's matching callbacks directly, since a
node never sends a publish back to the connection it came from.

If a subscription callback raises an exception, it's logged and the other
matching callbacks are still called. That includes the client's own
publishes: `publish()` doesn't pass a callback's exception on to its caller.

When a subscriber unsubscribes, each node tells a neighbour to stop sending
a subject as soon as no *other* connection on that node still wants it, so
subscriptions between nodes are torn down once the last subscriber anywhere
leaves. A node that joins the tree later (or any new connection) is sent the
subjects that already have subscribers.

Nodes must be connected as a tree, and they enforce it themselves: before
`establish_connection` returns, the node checks — by asking the nodes it's
already connected to — whether the node it just connected to can already be
reached another way. If so, the new connection would close a cycle, so it's
closed again and `establish_connection` raises `CycleCheckRefused` (a
`ConnectionError`, importable from `software_bus.pubsub`) saying why. A connection to the node itself, a second
connection to the same node, or to a peer that isn't a `PubSubNode` is
refused the same way. Every node in a tree has to be a version that does
this check. See "Cycle Prevention" in
[`docs/design/publish-subscribe.md`](docs/design/publish-subscribe.md) for the
full rules. Closing a node while `establish_connection` is still checking
makes it raise straight away, and so does the new connection dropping
during the check (a `ConnectionError` saying why), at whatever step the check is at.
Messages the new connection sends during the check are kept and processed
once it's accepted; if they come to more than `held_messages_limit` (64 MiB,
e.g. from a `bl_server` flooding its traffic), the connection is dropped and
the check ends at once. (Base-layer nodes don't check: a tree of `BaseLayerNode`s must
still be kept free of cycles by hand — `bus_check`, below, can find them.)

A `PubSubNode` treats a failed connection (peer dropped, or a send/receive
on it failed) as an implicit unsubscribe from every subject that connection
was subscribed to — exactly as if the peer had unsubscribed itself.
A malformed message (see [`docs/design/data-format.md`](docs/design/data-format.md))
is logged and ignored by both nodes and clients; the connection is kept.
A `PubSubClient` reports a lost connection to its node the same way a
`BaseLayerClient` does, via `register_connection_error_callback`.

#### Wire format

See [`docs/design/data-format.md`](docs/design/data-format.md) for the
canonical spec this section summarizes.

Each pub/sub message is sent as the base layer's `data` payload (see the
base layer wire format above — no additional outer framing is needed
here). The first byte is a message type tag:

| Message            | Byte layout                                                                 |
|--------------------|------------------------------------------------------------------------------|
| Subscription       | `0x01` \| `state` (1 byte: `0x01` subscribe / `0x00` unsubscribe) \| `subject_length` (2 bytes, big-endian) \| `subject` (UTF-8) |
| Publish            | `0x02` \| `subject_length` (2 bytes, big-endian) \| `subject` (UTF-8) \| `payload` (remaining bytes, arbitrary binary) |
| Hello              | `0x03` \| `node_id` (16 bytes) |
| Reachability Query | `0x04` \| `query_id` (16 bytes) \| `target_node_id` (16 bytes) |
| Reachability Reply | `0x05` \| `query_id` (16 bytes) \| `result` (1 byte: `0x00` not found / `0x01` found / `0x02` unknown) |

The last three are only used for cycle prevention: every node sends Hello
on each new connection (clients ignore it), and the reachability messages
only go between nodes.

`subject_length` caps subjects at 65535 UTF-8 bytes. A publish payload has
no length field of its own — it's simply everything left in the message
after the subject, since the outer base layer framing already delimits the
whole message. The whole message must fit the base layer's 16 MiB limit,
so a publish payload can be at most 16777216 − 3 − the subject's UTF-8
length; `publish` raises `PayloadTooLong` beyond that. Requests and replies
are checked the same way before anything is sent.

This is version 1 of the wire format, and it can only be extended in ways
older peers tolerate (see "Versions and Compatibility" in
[`docs/design/data-format.md`](docs/design/data-format.md)): a message with
an unknown type tag is ignored (logged only at debug level, and not passed
on), and bytes appended after the known fields of a Hello, reachability or
subscription message are ignored. Publish and request messages can't be
extended; changing them needs a new message type.

### `RequestReplyClient` — request/reply

A request/reply client built on top of `PubSubClient`; there's no separate
node class, since a `PubSubNode` routes requests and replies as ordinary
publishes. An acceptor registers a callback for a **request subject**; a
requester sends a request to that subject and gets the replies through its
own callback. A request subject is a pub/sub subject with no `*` wildcard;
one that's invalid or too long raises `ValueError`.

```python
import asyncio
from software_bus import PubSubNode, ReplyStatus, RequestReplyClient

async def main():
    node = PubSubNode()
    await node.accept_connection("127.0.0.1", 8787)

    replier = RequestReplyClient()
    await replier.connect("127.0.0.1", 8787)

    async def on_request(request_subject, reply_subject, payload):
        await replier.send_reply(reply_subject, payload.upper())

    await replier.accept_request("svc.echo", on_request)

    requester = RequestReplyClient()
    await requester.connect("127.0.0.1", 8787)

    def on_reply(reply_subject, payload, status):
        if status is ReplyStatus.REPLY:
            print(f"reply: {payload!r}")
            return True  # done: stop waiting for more replies
        print(f"no reply: {status.value}")  # timed out, closed or connection lost

    handle = await requester.send_request("svc.echo", b"hello", on_reply, timeout=10)

    # ... run for a while ...

    await requester.close()
    await replier.close()
    await node.close()

asyncio.run(main())
```

How a request's wait for replies works:

- Every acceptor on the subject gets each request, so there can be several
  replies. The reply callback is called with `(reply_subject, payload,
  status)` for each one, with `status` `ReplyStatus.REPLY`; returning a true
  value ends the wait.
- `timeout` is in seconds (default 300; `0` waits forever). When it runs
  out, the callback is called once more with an empty payload and
  `ReplyStatus.TIMED_OUT`. `close()` ends every pending request the same
  way with `ReplyStatus.CLOSED`, and losing the connection with
  `ReplyStatus.CONNECTION_LOST`, so a caller can tell a timeout from a
  lost connection.
- `send_request` returns the request's reply subject, which
  `cancel_request(handle)` takes to end the wait early, without calling the
  callback.
- The wait ends exactly once, whichever of these comes first; nothing is
  delivered after it.
- A client that accepts requests on a subject also receives its own
  requests to it, and its reply can arrive before `send_request` returns.

The request callback is called with `(request_subject, reply_subject,
payload)`, so one callback can accept requests on several subjects.
`stop_accept_request(subject, callback)` removes one acceptor, so keep a
reference to the function you accepted with. Callbacks can be sync or
async; one that raises is logged (a failing reply callback counts as
returning false, so the wait continues). Callbacks run on the client's
receive loop, so start long work as a separate task rather than awaiting it
in the callback. See
[`docs/design/request-reply.md`](docs/design/request-reply.md) for the full
rules.

#### Wire format

Requests and replies are ordinary pub/sub publishes, so no new message type
is needed; the subject says what a publish is:

| Published to                         | Payload                                                                     |
|--------------------------------------|-----------------------------------------------------------------------------|
| `_request.<request_subject>`         | `reply_subject_length` (2 bytes, big-endian) \| `reply_subject` (UTF-8) \| `request_payload` (remaining bytes) |
| `_reply.<request_subject>.<id>`      | the reply payload, as-is                                                    |

`<id>` is 32 random letters and digits, new for every request. Subjects
starting with `_request` or `_reply` are reserved for request/reply, so
don't publish or subscribe to them directly. An acceptor ignores (and logs)
a request whose reply subject isn't a reply subject for that request
subject, and `send_reply` only publishes to a valid reply subject. See
[`docs/design/data-format.md`](docs/design/data-format.md) for the
canonical spec.

## Command-line tools

`bl_server` runs a bus node: it connects to zero or more upstream servers
and/or listens for downstream connections.

```
python -m software_bus.bl_server --listen 127.0.0.1:8787
python -m software_bus.bl_server --upstream 10.0.0.1:8787 --listen 127.0.0.1:8787
```

`--upstream` and `--listen` may each be repeated to connect to, or listen
on, multiple addresses. Listening on an address other machines can reach
(anything but `localhost` or a loopback address such as 127.0.0.1) logs a
warning, since the bus has no authentication, but is allowed. The same goes
for `ps_server`.

If an upstream server can't be reached, or a `--listen` address can't be
used (e.g. it's already in use), `bl_server` prints
`error: cannot connect to <ip>:<port> (...)` or
`error: cannot listen on <ip>:<port> (...)` to stderr and exits with
status 1. Losing an upstream connection later doesn't stop it; it keeps
serving its other connections, but doesn't connect to that upstream again.

With `--reconnect true` it keeps trying instead: an upstream that can't be
reached, at start-up or after its connection is lost, is tried again after
1 second, then 2, 4 and so on up to 60 seconds between attempts (each a
little shortened at random, so servers that lost the same upstream don't
all retry at once). The delay goes back to 1 second after each successful
connection. The server listens straight away, without waiting for its
upstreams, and logs each failed attempt as a warning. An unusable
`--listen` address still exits.

```
python -m software_bus.bl_server --upstream 10.0.0.1:8787 --listen 127.0.0.1:8787 --reconnect true
```

`bl_client` connects to a bus node, optionally sends a message, then
prints any messages it receives in reply. `--message` defaults to `None`,
meaning it just connects and listens without sending anything:

```
python -m software_bus.bl_client --upstream 127.0.0.1:8787 --message "hello"
python -m software_bus.bl_client --upstream 127.0.0.1:8787  # listen only
```

The message can be sent more than once with `--repeat-count n` (default 1),
waiting `--repeat-interval t` seconds between sends (default 1):

```
python -m software_bus.bl_client --upstream 127.0.0.1:8787 --message "ping" \
    --repeat-count 5 --repeat-interval 0.5
```

Received messages are printed with a time stamp by default; pass
`--time-stamp false` to omit it:

```
python -m software_bus.bl_client --upstream 127.0.0.1:8787 --time-stamp false
```

If the server can't be reached, `bl_client` prints
`error: cannot connect to <ip>:<port> (...)` to stderr and exits with
status 1. Likewise if the connection is lost later — even partway through a
`--repeat-count` run — it prints `error: connection to server lost (...)`
and exits with status 1.

All the command-line tools accept `--debug true` to print `INFO`-level
logging (connection/disconnection events etc.) to stderr; it's off by
default:

```
python -m software_bus.bl_server --listen 127.0.0.1:8787 --debug true
```

If installed (`pip install -e .`), both are also available as the
`bl_server` and `bl_client` commands directly.

`ps_server` runs a publish/subscribe bus node, the same way `bl_server`
runs a plain one:

```
python -m software_bus.ps_server --listen 127.0.0.1:8787
python -m software_bus.ps_server --upstream 10.0.0.1:8787 --listen 127.0.0.1:8787
```

It reports an unreachable upstream or unusable `--listen` address the same
way as `bl_server`. An `--upstream` connection refused by the cycle check is
reported the same way too, with the reason, e.g.
`error: cannot connect to 127.0.0.1:8787 (refused by the cycle check: it would create a cycle)`.

`--reconnect true` works as for `bl_server`. A check that couldn't tell
whether there's a cycle (other links were being added at the same time) is
tried again like an unreachable upstream. Any other refusal on the first
attempt still exits as above. On a later attempt (e.g. the tree changed
while the upstream was down), the server logs it as an error, stops
trying that upstream, and keeps serving everything else. Subscriptions
need no help: both ends resend them on the new connection.

`ps_subscribe` connects to a `ps_server`, subscribes to one or more
required, comma-separated subjects (`--subject`; a subject may be a
wildcard pattern like `"a.*"`), and prints
`matched_subject actual_subject: payload` for each publish it receives,
with a time stamp by default (`--time-stamp false` to omit it):

```
python -m software_bus.ps_subscribe --upstream 127.0.0.1:8787 --subject "a.b,a.c"
```

Like `bl_client`, it exits with status 1 and an error message on stderr if
it can't connect to the server or the connection is lost. A subject over
65535 bytes is reported the same way, before connecting.

`ps_publish` connects to a `ps_server` and publishes a message under a
subject, `--repeat-count`/`--repeat-interval` times like `bl_client`.
`--subject` and `--message` are both required:

```
python -m software_bus.ps_publish --upstream 127.0.0.1:8787 --subject "a.b" --message "hello"
python -m software_bus.ps_publish --upstream 127.0.0.1:8787 --subject "a.b" --message "ping" \
    --repeat-count 5 --repeat-interval 0.5
```

It too exits with status 1 and an error message on stderr if it can't
connect, or if the connection is lost before it has finished publishing. A
subject with a `*` wildcard part, or over 65535 bytes, is reported the same
way, before connecting.

If installed (`pip install -e .`), these are also available as the
`ps_server`, `ps_subscribe`, and `ps_publish` commands directly.

`rr_reply` connects to a `ps_server`, accepts requests on one or more
required, comma-separated request subjects (`--accept-request`; no `*`
wildcards), prints `request_subject: request_payload` for each request it
receives (with a time stamp by default; `--time-stamp false` to omit it),
and replies with `Reply for request_subject: reply_message` (`--reply-message`,
required):

```
python -m software_bus.rr_reply --upstream 127.0.0.1:8787 --accept-request "svc.echo,svc.time" --reply-message "pong"
```

`rr_request` connects to a `ps_server`, sends one request to
`--request-subject` (required) with `--request-message` (default: empty),
and prints `request_subject: reply_payload` for each reply:

```
$ python -m software_bus.rr_request --upstream 127.0.0.1:8787 --request-subject svc.echo --request-message ping --continue-waiting false
[2026-10-12T16:51:13.801415] svc.echo: Reply for svc.echo: pong
```

- `--continue-waiting true` (the default) keeps printing replies until
  `--timeout` seconds (default 300; `0` waits forever, until Ctrl-C), then
  prints `request_subject: timed out`. `--continue-waiting false` exits
  after the first reply.
- It exits with status 0 if it got at least one reply, and with status 1
  (`error: no reply to 'svc.echo' before the timeout`) if the timeout ends
  the wait with none.

Both exit with status 1 and an error message on stderr for an invalid
request subject (checked before connecting), if they can't connect to the
server, or if the connection is lost; a lost connection is never reported
as a timeout. If installed, they're also available as the `rr_reply` and
`rr_request` commands.

### Checking for cycles: `bus_check`

`bus_check` finds cycles among the `bl_server`/`ps_server` nodes running on
this machine, from their TCP connections as the operating system reports
them — it sends nothing over the bus. It's mainly for base-layer networks,
which, unlike pub/sub nodes, don't refuse a connection that closes a cycle:

```
$ python -m software_bus.bus_check
Nodes found: 3
  pid 26932 (bl_server, 127.0.0.1:8787)
  pid 26564 (bl_server, 127.0.0.1:8788)
  pid 15368 (bl_server, 127.0.0.1:8789)
Links between them: 3
Cycles found: 1
  pid 26932 (bl_server, 127.0.0.1:8787) - pid 15368 (bl_server, 127.0.0.1:8789) - pid 26564 (bl_server, 127.0.0.1:8788) - back to pid 26932
```

It exits with status 0 if there's no cycle, 1 if there is, and 2 if the
check can't run. Clients are ignored (a process with one connection can't
be part of a cycle). A program that runs a node itself, rather than through
`bl_server`/`ps_server`, can be included with `--pid PID` (repeatable).
Only this machine is checked: connections to other machines are counted in
a warning but not followed, as are node connections whose other end the
operating system didn't list (e.g. one that closed during the check).

It also warns when the operating system hides something the check needs: a
node connection whose other end belongs to a process you can't see, or a
node whose sockets you can't see at all (e.g. a node run by another user on
Linux). Run it as administrator/root to see everything. See
[`docs/design/bus-check.md`](docs/design/bus-check.md).

It needs `psutil`, an optional dependency:

```
python3 -m pip install --user -e ".[check]"   # or just: python3 -m pip install --user psutil
```

Reinstalling also adds the `bus_check` command.

## Project layout

```
docs/design/    design docs
docs/test-cases.md  every test, in plain English (plus manual tests)
docs/development-approach.md   how this project is developed and tested
docs/development-guideline.md  the same, condensed and project-neutral
docs/known-issues.md  limitations deliberately left unfixed
src/software_bus/
    base_layer.py   BaseLayerNode, Connection
    client.py       BaseLayerClient
    pubsub.py       PubSubNode, PubSubClient
    request_reply.py  RequestReplyClient
    subject_matcher.py  SubjectMatcher, ExactStringMatcher, StringPatternMatcher
    bl_server.py    bl_server CLI
    bl_client.py    bl_client CLI
    ps_server.py    ps_server CLI
    ps_subscribe.py ps_subscribe CLI
    ps_publish.py   ps_publish CLI
    rr_reply.py     rr_reply CLI
    rr_request.py   rr_request CLI
    bus_check.py    bus_check CLI (finds cycles among local nodes)
    _cli.py         helpers shared by the CLI scripts
tests/          pytest test suite
```

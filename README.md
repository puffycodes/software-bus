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
[`docs/design/subject-matcher.md`](docs/design/subject-matcher.md)). The
wire formats for both layers are specified in
[`docs/design/data-format.md`](docs/design/data-format.md).

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

When a connection drops, or a send/receive on it fails, it's removed from
`upstream_connections`/`downstream_connections` and an upstream or
downstream connection error callback is called with the `Connection` and
the exception (`None` on a clean shutdown); once the callback returns, the
connection is closed, so don't keep it around to send on. The default callbacks just log
the event; override them with `register_upstream_connection_error_callback`
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
re-raises the error to the caller. The default callback just logs; register
your own to react, e.g. to reconnect:

```python
client.register_connection_error_callback(
    lambda connection, error: print("lost connection:", connection.address, error)
)
```

Closing the client yourself with `close()` doesn't call the callback.

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
always literal. Matching is done by `StringPatternMatcher`
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
registered on a given subject. `unsubscribe(subject, callback)` removes one
callback, only telling the upstream node once no callback remains for that
subject:

```python
await subscriber.unsubscribe("a.b", callback)
```

A client that is subscribed to a subject it publishes to receives its own
publish: `publish()` calls the client's matching callbacks directly, since a
node never sends a publish back to the connection it came from.

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
`ConnectionError`) saying why. A connection to the node itself, a second
connection to the same node, or to a peer that isn't a `PubSubNode` is
refused the same way. Every node in a tree has to be a version that does
this check. See "Cycle Prevention" in
[`docs/design/publish-subscribe.md`](docs/design/publish-subscribe.md) for the
full rules. (Base-layer nodes don't check: a tree of `BaseLayerNode`s must
still be kept free of cycles by hand.)

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

| Message      | Byte layout                                                                 |
|--------------|------------------------------------------------------------------------------|
| Subscription | `0x01` \| `state` (1 byte: `0x01` subscribe / `0x00` unsubscribe) \| `subject_length` (2 bytes, big-endian) \| `subject` (UTF-8) |
| Publish      | `0x02` \| `subject_length` (2 bytes, big-endian) \| `subject` (UTF-8) \| `payload` (remaining bytes, arbitrary binary) |

`subject_length` caps subjects at 65535 UTF-8 bytes. A publish payload has
no length field of its own — it's simply everything left in the message
after the subject, since the outer base layer framing already delimits the
whole message.

## Command-line tools

`bl_server` runs a bus node: it connects to zero or more upstream servers
and/or listens for downstream connections.

```
python -m software_bus.bl_server --listen 127.0.0.1:8787
python -m software_bus.bl_server --upstream 10.0.0.1:8787 --listen 127.0.0.1:8787
```

`--upstream` and `--listen` may each be repeated to connect to, or listen
on, multiple addresses.

If an upstream server can't be reached, or a `--listen` address can't be
used (e.g. it's already in use), `bl_server` prints
`error: cannot connect to <ip>:<port> (...)` or
`error: cannot listen on <ip>:<port> (...)` to stderr and exits with
status 1. Losing an upstream connection later doesn't stop it; it keeps
serving its other connections.

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

All five command-line tools accept `--debug true` to print `INFO`-level
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

`ps_subscribe` connects to a `ps_server`, subscribes to one or more
required, comma-separated subjects (`--subject`; a subject may be a
wildcard pattern like `"a.*"`), and prints
`matched_subject actual_subject: payload` for each publish it receives,
with a time stamp by default (`--time-stamp false` to omit it):

```
python -m software_bus.ps_subscribe --upstream 127.0.0.1:8787 --subject "a.b,a.c"
```

Like `bl_client`, it exits with status 1 and an error message on stderr if
it can't connect to the server or the connection is lost.

`ps_publish` connects to a `ps_server` and publishes a message under a
subject, `--repeat-count`/`--repeat-interval` times like `bl_client`.
`--subject` and `--message` are both required:

```
python -m software_bus.ps_publish --upstream 127.0.0.1:8787 --subject "a.b" --message "hello"
python -m software_bus.ps_publish --upstream 127.0.0.1:8787 --subject "a.b" --message "ping" \
    --repeat-count 5 --repeat-interval 0.5
```

It too exits with status 1 and an error message on stderr if it can't
connect, or if the connection is lost before it has finished publishing.

If installed (`pip install -e .`), these are also available as the
`ps_server`, `ps_subscribe`, and `ps_publish` commands directly.

## Project layout

```
docs/design/    design docs
src/software_bus/
    base_layer.py   BaseLayerNode, Connection
    client.py       BaseLayerClient
    pubsub.py       PubSubNode, PubSubClient
    subject_matcher.py  SubjectMatcher, ExactStringMatcher, StringPatternMatcher
    bl_server.py    bl_server CLI
    bl_client.py    bl_client CLI
    ps_server.py    ps_server CLI
    ps_subscribe.py ps_subscribe CLI
    ps_publish.py   ps_publish CLI
    _cli.py         helpers shared by the CLI scripts
tests/          pytest test suite
```

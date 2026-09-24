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
the publish/subscribe layer built on top of it. The wire formats for both
layers are specified in [`docs/design/data-format.md`](docs/design/data-format.md).

## Install

```
pip install --user -e ".[dev]"
```

If your `pip`/`setuptools` version doesn't support editable installs for
this `pyproject.toml`, run code and tests with `src/` on the path instead:

```
PYTHONPATH=src python3 -m pytest
```

## Running the tests

```
PYTHONPATH=src python3 -m pytest -v
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
the exception (`None` on a clean shutdown). The default callbacks just log
the event; override them with `register_upstream_connection_error_callback`
/ `register_downstream_connection_error_callback` to react instead:

```python
hub.register_downstream_connection_error_callback(
    lambda connection, error: print("downstream peer gone:", connection.address, error)
)
```

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

### `PubSubNode` and `PubSubClient` — publish/subscribe

A publish/subscribe layer built on top of `BaseLayerNode`/`BaseLayerClient`.
A **subject** is a `.`-separated string (e.g. `"a.b"`). A `PubSubClient`
subscribes to a subject to receive future publishes tagged with it, and
publishes payloads under a subject; a `PubSubNode` tracks, per subject,
which of its connections are interested and only forwards a publish to
those, propagating subscribe/unsubscribe through the tree as needed.

```python
import asyncio
from software_bus import PubSubClient, PubSubNode

async def main():
    node = PubSubNode()
    await node.accept_connection("127.0.0.1", 8787)

    subscriber = PubSubClient()
    subscriber.register_publish_callback(
        lambda subject, payload: print(f"{subject}: {payload!r}")
    )
    await subscriber.connect("127.0.0.1", 8787)
    await subscriber.subscribe("a.b")

    publisher = PubSubClient()
    await publisher.connect("127.0.0.1", 8787)
    await publisher.publish("a.b", b"hello")

    # ... run for a while ...

    await subscriber.close()
    await publisher.close()
    await node.close()

asyncio.run(main())
```

Unsubscribe by passing `subscribe=False`: `await subscriber.subscribe("a.b", subscribe=False)`.
A `PubSubClient` can also react to subscription traffic with
`register_subscribe_callback(lambda subject, state: ...)`, called with
`state` `True` for subscribe and `False` for unsubscribe.

A `PubSubNode` treats a failed connection (peer dropped, or a send/receive
on it failed) as an implicit unsubscribe from every subject that connection
was subscribed to: it's removed from each subject, and for any subject left
with no subscribers on either side, an unsubscribe is propagated to the
remaining connections — exactly as if the peer had unsubscribed itself.

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

`ps_subscribe` connects to a `ps_server`, subscribes to one or more
required, comma-separated subjects (`--subject`), and prints
`subject: payload` for each publish it receives, with a time stamp by
default (`--time-stamp false` to omit it):

```
python -m software_bus.ps_subscribe --upstream 127.0.0.1:8787 --subject "a.b,a.c"
```

`ps_publish` connects to a `ps_server` and publishes a message under a
subject, `--repeat-count`/`--repeat-interval` times like `bl_client`.
`--subject` and `--message` are both required:

```
python -m software_bus.ps_publish --upstream 127.0.0.1:8787 --subject "a.b" --message "hello"
python -m software_bus.ps_publish --upstream 127.0.0.1:8787 --subject "a.b" --message "ping" \
    --repeat-count 5 --repeat-interval 0.5
```

If installed (`pip install -e .`), these are also available as the
`ps_server`, `ps_subscribe`, and `ps_publish` commands directly.

## Project layout

```
docs/design/    design docs
src/software_bus/
    base_layer.py   BaseLayerNode, Connection
    client.py       BaseLayerClient
    pubsub.py       PubSubNode, PubSubClient
    bl_server.py    bl_server CLI
    bl_client.py    bl_client CLI
    ps_server.py    ps_server CLI
    ps_subscribe.py ps_subscribe CLI
    ps_publish.py   ps_publish CLI
    _cli.py         helpers shared by the CLI scripts
tests/          pytest test suite
```

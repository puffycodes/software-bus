# software-bus

A layered software bus for connecting distributed instances over TCP.

Instances connect to each other as **upstream** (toward the hub/root) and
**downstream** (branching out) peers, forming a tree. Data is flooded to
every other connection: anything received from a downstream connection is
relayed to all upstream connections and to every other downstream
connection; anything received from an upstream connection is relayed to
all downstream connections and to every other upstream connection.

See [`docs/design/base-layer.md`](docs/design/base-layer.md) for the full
design and routing rules.

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

If installed (`pip install -e .`), both are also available as the
`bl_server` and `bl_client` commands directly.

## Project layout

```
docs/design/    design docs
src/software_bus/
    base_layer.py   BaseLayerNode, Connection
    client.py       BaseLayerClient
    bl_server.py    bl_server CLI
    bl_client.py    bl_client CLI
tests/          pytest test suite
```

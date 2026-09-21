# software-bus

A layered software bus for connecting distributed instances over TCP.

Instances connect to each other as **upstream** (toward the hub/root) and
**downstream** (branching out) peers, forming a tree. Data sent into the
bus is flooded outward: anything received from a downstream connection is
relayed to all upstream connections and to every other downstream
connection; anything received from an upstream connection is relayed to
all downstream connections.

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

### `BaseLayer` — a bus node

Accepts downstream connections on one or more IP/port pairs, can connect
out to an upstream instance, and relays data per the routing rule above.

```python
import asyncio
from software_bus import BaseLayer

async def main():
    hub = BaseLayer()
    await hub.accept_connection("127.0.0.1", 8787)  # listen for downstream peers
    await hub.establish_connection("10.0.0.1", 8787)  # connect to an upstream peer

    # ... run for a while ...

    await hub.close()

asyncio.run(main())
```

### `BaseLayerClient` — a leaf client

A plain point-to-point client for talking to a `BaseLayer` node: connect,
send, and register a callback for incoming data. A `BaseLayer` never
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

## Project layout

```
docs/design/    design docs
src/software_bus/
    base_layer.py   BaseLayer, Connection
    client.py       BaseLayerClient
tests/          pytest test suite
```

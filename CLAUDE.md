# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

There is no `python` on PATH in this environment — use `python3` explicitly.

```
# Install (editable install currently fails: this pyproject.toml has no setup.py,
# and `pip install -e .` requires one in this environment). Use PYTHONPATH instead:
PYTHONPATH=src python3 -m pytest            # run the full test suite
PYTHONPATH=src python3 -m pytest -v
PYTHONPATH=src python3 -m pytest tests/test_base_layer.py::test_close_clears_connection_lists_and_addresses  # single test
PYTHONPATH=src python3 -m pytest tests/test_pubsub.py -v   # single file

# Run a CLI tool directly against the source tree, same way:
PYTHONPATH=src python3 -m software_bus.bl_server --listen 127.0.0.1:8787
```

No linter/formatter is configured in this repo.

## Architecture

This is a two-layer TCP bus, where each layer has a **Node** class (a relay/hub, accepts downstream + connects upstream) and a **Client** class (a leaf, connects to one node). The design docs in `docs/design/base-layer.md` and `docs/design/publish-subscribe.md` are the spec these classes implement — when those docs change, `src/software_bus/base_layer.py` / `pubsub.py` (and the corresponding CLI scripts) need to be updated to match, and vice versa.

### Base layer (`base_layer.py`, `client.py`)

`BaseLayerNode` instances form a tree: each accepts TCP connections from **downstream** peers on any number of IP/port pairs (`accept_connection`) and can open connections to **upstream** peers (`establish_connection`), tracking the two in separate lists (`upstream_connections` / `downstream_connections`). Data flooding is the default behavior: anything received from a downstream connection is relayed to all upstream connections and every other downstream connection, and symmetrically for upstream. This routing is implemented as two overridable callbacks (`register_upstream_receive_callback` / `register_downstream_receive_callback`) rather than hardcoded — the pub/sub layer replaces both to implement its own routing instead of flooding.

Every connection (`Connection` dataclass) is framed the same way regardless of what's on top: a 4-byte big-endian length prefix followed by exactly that many payload bytes (`_LENGTH_PREFIX_SIZE`), since raw TCP has no message boundaries.

Connection lifecycle and error handling follows one path no matter where the failure happens: `_relay_loop` (background task per connection, reading and dispatching to the receive callback) and `_relay_to` (used when fanning data out to peers) both funnel every failure — peer dropped, receive failed, send failed — into a single `_handle_connection_error` helper. That helper removes the connection from its list and invokes the registered upstream/downstream *connection error callback* (`register_upstream_connection_error_callback` / `register_downstream_connection_error_callback`; defaults just log). When extending failure handling, hook into `_handle_connection_error` rather than adding ad hoc handling at each call site.

Shared internals to reuse rather than re-inline: `_start_connection` (wraps a new socket in a `Connection`, appends it to the right list, starts its `_relay_loop`), `_connections(from_upstream)` (the matching list), `_peers_except(source)` (every connection but the sender — the flooding target set, also used by pub/sub), and the module-level `_maybe_await` (every user callback may be sync or async; `client.py` and `pubsub.py` import it too).

`BaseLayerClient` is the leaf side: connects once, sends, and dispatches received data to a single registered callback.

### Publish/Subscribe layer (`pubsub.py`)

`PubSubNode` and `PubSubClient` are built by **composing** a `BaseLayerNode`/`BaseLayerClient` internally (not subclassing) and registering pub/sub-specific receive and connection error callbacks on it. The pub/sub wire format is a message-type tag (`0x01` subscription / `0x02` publish) prefixing the base layer's opaque payload — `encode_message`/`decode_message` handle this framing, which lives entirely inside the base layer's payload (no separate outer framing needed).

`PubSubNode` tracks subscriber interest *per subject* in two dicts (`_downstream_subscriptions`, `_upstream_subscriptions`, each `subject -> [Connection]`), propagating subscribe/unsubscribe messages through the tree (to the opposite side always, and to the same side except the sender) and only forwarding a given publish to connections actually subscribed to that subject — this is the key difference from the base layer's unconditional flooding.

All `PubSubNode` sends go through the base layer's `_relay_to` (via `_send_to`), so send failures reach `_handle_connection_error` like any other. Its connection error callbacks treat a failed connection as an implicit unsubscribe from every subject it held (`_drop_connection`), sharing `_unsubscribe` with explicit unsubscribe messages: once a subject has no subscribers on either side, the unsubscribe is propagated to all remaining connections.

### CLI scripts

`bl_server`/`bl_client`/`ps_server`/`ps_subscribe`/`ps_publish` are thin argparse wrappers around the classes above; shared parsing lives in `_cli.py` (`parse_address` for `ip:port`, `parse_bool` for `true|false` flags, `parse_subject_list` for comma-separated subjects, `configure_logging` for the shared `--debug` flag). Each script's `run(...)` coroutine is unit-tested directly (see `tests/test_scripts.py`, `tests/test_ps_scripts.py`) separately from its `build_arg_parser()`.

### Testing conventions

Tests exercise real TCP: `tests/helpers.py` provides `open_peer_connection`/`accept_one_peer_connection`/`free_port` to stand up raw peers on ephemeral ports rather than mocking sockets. `pytest-asyncio` runs in `asyncio_mode = "auto"` (see `pyproject.toml`), but tests still mark async tests with `@pytest.mark.asyncio` by convention — follow the existing style. Most async tests bind to port `0` to get an OS-assigned free port and avoid clashing with anything else listening on the machine.

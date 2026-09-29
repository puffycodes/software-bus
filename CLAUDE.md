# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

The interpreter name depends on the machine this shared checkout is used from:

- **Linux VM:** there is no `python` on PATH — use `python3` explicitly (as in the commands below).
- **Windows host:** `python3` is only the Microsoft Store stub ("Python was not found") — use `python`, which resolves to the `standard-dev-3.13` virtualenv (Python 3.13). Substitute `python` for `python3` in the commands below.

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

This is a two-layer TCP bus, where each layer has a **Node** class (a relay/hub, accepts downstream + connects upstream) and a **Client** class (a leaf, connects to one node). The design docs in `docs/design/base-layer.md` and `docs/design/publish-subscribe.md` are the spec these classes implement, `docs/design/subject-matcher.md` is the spec for how a subscribed subject is matched against a published one, and `docs/design/data-format.md` is the spec for the wire formats they encode/decode — when those docs change, `src/software_bus/base_layer.py` / `pubsub.py` / `subject_matcher.py` (and the corresponding CLI scripts) need to be updated to match, and vice versa.

### Base layer (`base_layer.py`, `client.py`)

`BaseLayerNode` instances form a tree: each accepts TCP connections from **downstream** peers on any number of IP/port pairs (`accept_connection`) and can open connections to **upstream** peers (`establish_connection`), tracking the two in separate lists (`upstream_connections` / `downstream_connections`). Data flooding is the default behavior: anything received from a downstream connection is relayed to all upstream connections and every other downstream connection, and symmetrically for upstream. This routing is implemented as two overridable callbacks (`register_upstream_receive_callback` / `register_downstream_receive_callback`) rather than hardcoded — the pub/sub layer replaces both to implement its own routing instead of flooding.

Every connection (`Connection` dataclass) is framed the same way regardless of what's on top: a 4-byte big-endian length prefix followed by exactly that many payload bytes (`_LENGTH_PREFIX_SIZE`), since raw TCP has no message boundaries.

Connection lifecycle and error handling follows one path no matter where the failure happens: `_relay_loop` (background task per connection, reading and dispatching to the receive callback) and `_relay_to` (used when fanning data out to peers) both funnel every failure — peer dropped, receive failed, send failed — into a single `_handle_connection_error` helper. That helper removes the connection from its list, invokes the registered upstream/downstream *connection error callback* (`register_upstream_connection_error_callback` / `register_downstream_connection_error_callback`; defaults just log), and then closes the connection. Closing matters: since Python 3.12.1, `Server.wait_closed()` blocks until every connection the server accepted is closed, so a dropped-but-unclosed connection would hang `BaseLayerNode.close()` forever. For the same reason `close()` closes all connections *before* awaiting `wait_closed()`. `Connection.close()` skips cancelling its relay task when called from that task, because `_handle_connection_error` usually runs inside `_relay_loop`. When extending failure handling, hook into `_handle_connection_error` rather than adding ad hoc handling at each call site.

Shared internals to reuse rather than re-inline: `_start_connection` (wraps a new socket in a `Connection`, appends it to the right list, starts its `_relay_loop`), `_connections(from_upstream)` (the matching list), `_peers_except(source)` (every connection but the sender — the flooding target set, also used by pub/sub), and the module-level `_maybe_await` (every user callback may be sync or async; `client.py` and `pubsub.py` import it too).

`BaseLayerClient` is the leaf side: connects once, sends, and dispatches received data to a single registered callback. Its failures mirror the node's single path: `_receive_loop` and `send` both go through the client's own `_handle_connection_error` (forget `self.connection`, call the connection error callback, close), and `send` re-raises after reporting. A `CancelledError` in `_receive_loop` means `close()` was called and is deliberately *not* reported. `PubSubClient.register_connection_error_callback` just forwards to it.

Subjects are capped at 65535 UTF-8 bytes by the 2-byte length field; `encode_message` raises `ValueError` beyond that, and `PubSubClient.subscribe` encodes *before* registering the callback so a rejected subject leaves no state behind.

### Publish/Subscribe layer (`pubsub.py`)

`PubSubNode` and `PubSubClient` are built by **composing** a `BaseLayerNode`/`BaseLayerClient` internally (not subclassing) and registering pub/sub-specific receive and connection error callbacks on it. The pub/sub wire format is a message-type tag (`0x01` subscription / `0x02` publish) prefixing the base layer's opaque payload — `encode_message`/`decode_message` handle this framing, which lives entirely inside the base layer's payload (no separate outer framing needed).

`PubSubNode` tracks subscriber interest *per subscribed subject* in two dicts (`_downstream_subscriptions`, `_upstream_subscriptions`, each `subscribed_subject -> [Connection]`), propagating subscribe/unsubscribe messages through the tree (to the opposite side always, and to the same side except the sender) and only forwarding a given publish to connections actually subscribed to that subject — this is the key difference from the base layer's unconditional flooding. A subscribed subject may be a wildcard pattern (e.g. `a.*`), so routing a publish is not a dict lookup by exact key: `_matching_connections` runs every tagged subject in a dict through a `StringPatternMatcher` (`subject_matcher.py`) against the published subject and collects the connections under every match. `PubSubClient._deliver` does the analogous match over its own `_subscribe_callbacks` keys before invoking callbacks. Subscribe/unsubscribe bookkeeping itself (`_add_subscriber`/`_discard_subscriber`/`_unsubscribe`) still keys and compares by the literal subscribed-subject string — only publish routing needs the matcher.

Routing rules that look simplifiable but aren't (see `publish-subscribe.md`; nodes must form a tree):
- `_handle_publish` never sends a publish back to its `source`. Two neighbouring nodes that both have subscribers tag *each other* as subscribers, so without this a single publish ping-pongs between them forever. Because of this, `PubSubClient.publish` calls its own matching callbacks locally (`_deliver`) — a client receiving its own publishes is intended behaviour.
- `_unsubscribe` decides per neighbour: a connection is told "unsubscribe" once no *other* connection still subscribes (no subscribers left → everyone but the sender; exactly one left → just that one). A plain "subject now empty on this node?" check never fires when two nodes tag each other, leaving subscriptions up forever. It acts only when `_discard_subscriber` actually removed something, otherwise redundant unsubscribes could bounce between nodes.
- `_on_new_connection` (registered via the base layer's `register_upstream/downstream_new_connection_callback`) sends every subject with subscribers to a newly joined connection, so a node that joins late still learns existing subscriptions. Clients get these too and just log them.
- `decode_message` raises `ValueError` for every malformed payload (`data-format.md`); both `PubSubNode._on_receive` and `PubSubClient._on_receive` log and skip such a message rather than dropping the connection.

All `PubSubNode` sends go through the base layer's `_relay_to` (via `_send_to`), so send failures reach `_handle_connection_error` like any other. Its connection error callbacks treat a failed connection as an implicit unsubscribe from every subject it held (`_drop_connection`), sharing `_unsubscribe` (and its per-neighbour rule above) with explicit unsubscribe messages.

### CLI scripts

`bl_server`/`bl_client`/`ps_server`/`ps_subscribe`/`ps_publish` are thin argparse wrappers around the classes above; everything they share lives in `_cli.py`: value parsers (`parse_address` for `ip:port`, `parse_bool` for `true|false` flags, `parse_subject_list` for comma-separated subjects), argument builders (`add_debug_argument`, `add_upstream_argument`, `add_repeat_arguments`, and `build_server_arg_parser` for both servers), and runtime helpers (`configure_logging`, `run_server`, `repeat`, `print_received`, `run_until_connection_lost`, `run_until_interrupted`). Client scripts that stay running (`bl_client`, `ps_subscribe`) wrap their body in `run_until_connection_lost`, which races it against the client's connection error callback and raises `ConnectionLost`; `run_until_interrupted` turns that into an error message and exit status 1. `bl_server`/`ps_server` differ only in the node class they pass to `run_server`. Add new shared options/behaviour there rather than copying it into each script. Each script's `run(...)` coroutine is unit-tested directly (see `tests/test_scripts.py`, `tests/test_ps_scripts.py`) separately from its `build_arg_parser()`; the shared `_cli.py` helpers, and options every script must accept (e.g. `--debug`), are tested in `tests/test_cli.py`.

### Testing conventions

Tests exercise real TCP: `tests/helpers.py` provides `open_peer_connection`/`accept_one_peer_connection`/`free_port` to stand up raw peers on ephemeral ports rather than mocking sockets. The server returned by `accept_one_peer_connection` has a patched `wait_closed()` that first closes the peer connection it accepted; without it, test teardown hangs on Python 3.12.1+ (see the `wait_closed` note above). `pytest-asyncio` runs in `asyncio_mode = "auto"` (see `pyproject.toml`), but tests still mark async tests with `@pytest.mark.asyncio` by convention — follow the existing style. Most async tests bind to port `0` to get an OS-assigned free port and avoid clashing with anything else listening on the machine.

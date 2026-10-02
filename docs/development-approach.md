# Development and Testing Approach

How this project is developed and tested. For the commands to install and run things, see [`README.md`](../README.md); for the architecture, see [`CLAUDE.md`](../CLAUDE.md) and the specs in [`design/`](design/). A condensed, project-neutral version of this document, for reuse in other projects, is [`development-guideline.md`](development-guideline.md).

## Development approach

### Spec-first design docs
- `docs/design/` holds one spec per area: `base-layer.md`, `publish-subscribe.md`, `subject-matcher.md`, `data-format.md` and `bus-check.md`. Each maps to one module.
- The specs and the code are kept in step in both directions. A change to the spec means a matching change to the code, and the reverse. A single change usually updates the spec, the code, the tests and `test-cases.md` together.
- The specs record the reasons behind rules that look like they could be simplified, such as never sending a publish back to where it came from, or deciding per neighbour when to unsubscribe. This stops someone "cleaning them up" later.

### Layered architecture built by composition
- There are two layers: a base layer that floods data, and a pub/sub layer that routes it. Each layer has a Node class and a Client class.
- The pub/sub classes contain a base-layer object and register callbacks on it, instead of subclassing it. Routing is swapped out through those callbacks, not hardcoded.

### Shared internals instead of copies
- Every failure goes through one place: `_handle_connection_error`.
- Common helpers are reused rather than rewritten: `_start_connection`, `_peers_except`, `_maybe_await`, `_send_to`/`_relay_to`.
- Everything the CLI scripts share lives in `_cli.py`. Each script is a thin argparse wrapper around a testable `run(...)` coroutine.
- Script errors are subclasses of `ScriptError`, which a single wrapper turns into a one-line message and exit status 1. `bus_check` is the documented exception: it has its own exit codes.

### Error-handling rules
- An exception in a receive callback loses only that message, never the connection.
- Malformed messages raise `ValueError`, which is logged and the message skipped.
- Inputs are checked before any state is changed. For example, `subscribe` encodes the subject before registering the callback, so a rejected subject leaves nothing behind.

### Portability and dependencies
- Supports Python 3.8 (WSL, Ubuntu 20.04) up to 3.13 (Windows).
- No runtime dependencies. `psutil` is an optional `check` extra and is imported only inside `bus_check.collect()`.
- Installed as an editable package from `pyproject.toml` (no `setup.py`, so pip ≥ 21.3 is needed). `PYTHONPATH=src` is the fallback when the package isn't installed.
- Behaviour that differs between Python versions and operating systems is handled and documented:
  - From 3.12.1, `Server.wait_closed()` hangs until every connection is closed.
  - On Windows, `bl_server.exe` is a launcher that runs the real process as a child.
  - On Windows, asyncio keeps a loopback connection to itself in every process.

### Documentation upkeep
- `CLAUDE.md` and `README.md` are updated along with each feature.
- No linter or formatter is configured.
- Features are built up in steps: base layer → scripts → pub/sub → error handling → wildcard subjects → cycle prevention → `bus_check`.

## Testing approach

### Tools
- pytest with pytest-asyncio in `asyncio_mode = "auto"`, with a fresh event loop for each test.
- Async tests are still marked `@pytest.mark.asyncio` by convention.

### Real network, no socket mocks
- `tests/helpers.py` sets up raw peers on ports the OS assigns: `open_peer_connection`, `accept_one_peer_connection`, `free_port`.
- `accept_one_peer_connection` patches `wait_closed()` so test teardown doesn't hang on Python 3.12.1+.
- Helpers let a raw peer play along with cycle prevention: `expect_hello`, `greet_as_node`, `answer_query`, `establish_to_raw_peer`.

### Timing
- Every wait is bounded with `asyncio.wait_for(..., timeout=...)`.
- To check that something does *not* arrive, a test waits a short time (0.1–0.2 s) and expects a timeout.
- Protocol timings are class attributes (`check_timeout`, `check_retry_delay`, `max_check_attempts`), and tests shrink them on each instance.

### Where fakes are used
- Fakes are used only where real resources are impractical. `bus_check` logic is pure and tested with fake `TcpConnection`s, and `collect()` is tested by putting a fake `psutil` into `sys.modules`.
- Only two tests start real `bl_server` processes, and they skip themselves if `psutil` isn't installed (`importorskip("psutil")`).

### Tests per layer
- Library classes, the shared CLI helpers (`test_cli.py`), and each script's `run()` coroutine are tested separately. Script tests check printed output with `capsys`; logging is checked with `caplog`.
- Options every script must accept (such as `--debug`) are tested across all scripts.
- Inputs are table-driven with `parametrize` where it fits.

### Regression tests for subtle fixes
- Each concurrency safeguard has a test that fails without it, for example `_check_lock` and answering "unknown" while a check is running.

### The test catalogue
- [`test-cases.md`](test-cases.md) describes every automated test in plain English, grouped by file with a count for each.
- It must be updated in the same change whenever tests are added, removed or changed. The total is checked with `python -m pytest --collect-only -q`.
- A "Manual tests" section covers what can't be automated, such as a socket owned by another user that needs `sudo`. Each manual test lists what it needs, the steps, the expected result, clean-up, and the date and result of its last run. If a change touches what a manual test covers, it is flagged for re-running rather than its "Last run" being updated.

### Running the tests
- `python3 -m pytest` on WSL, `python -m pytest` on Windows. A single test or a single file can be run on its own, e.g. `python3 -m pytest tests/test_pubsub.py -v`.

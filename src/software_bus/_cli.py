from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from typing import Any, Awaitable, Callable, Coroutine, List, Optional, Tuple

from .base_layer import DEFAULT_HOST, DEFAULT_PORT


def configure_logging(debug: bool) -> None:
    """Enable INFO-level logging output when --debug is set."""
    if debug:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )


def parse_address(value: str) -> Tuple[str, int]:
    """Parse an "ip:port" command-line argument."""
    host, sep, port = value.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected ip:port, got {value!r}")
    return host, int(port)


def parse_bool(value: str) -> bool:
    """Parse a "true"/"false" command-line argument."""
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise argparse.ArgumentTypeError(f"expected true|false, got {value!r}")


def parse_subject_list(value: str) -> List[str]:
    """Parse a comma-separated "<subject_1>,<subject_2>,..." argument."""
    subjects = [subject.strip() for subject in value.split(",")]
    subjects = [subject for subject in subjects if subject]
    if not subjects:
        raise argparse.ArgumentTypeError(f"expected one or more subjects, got {value!r}")
    return subjects


def add_debug_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug",
        type=parse_bool,
        default=False,
        metavar="true|false",
        help="print logging information (default: false)",
    )


def add_upstream_argument(parser: argparse.ArgumentParser, server_name: str) -> None:
    """Add a single --upstream ip:port argument for a client script."""
    parser.add_argument(
        "--upstream",
        metavar="ip:port",
        type=parse_address,
        default=(DEFAULT_HOST, DEFAULT_PORT),
        help=f"{server_name} to connect to (default: {DEFAULT_HOST}:{DEFAULT_PORT})",
    )


def add_repeat_arguments(parser: argparse.ArgumentParser, action: str) -> None:
    """Add --repeat-count / --repeat-interval; `action` is e.g. "send"."""
    parser.add_argument(
        "--repeat-count",
        type=int,
        default=1,
        metavar="n",
        help=f"number of times to {action} the message (default: 1)",
    )
    parser.add_argument(
        "--repeat-interval",
        type=float,
        default=1.0,
        metavar="t",
        help=f"seconds to wait between repeated {action}s (default: 1)",
    )


def build_server_arg_parser(description: str, server_name: str) -> argparse.ArgumentParser:
    """Argument parser shared by the bl_server and ps_server scripts."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--upstream",
        metavar="ip:port",
        type=parse_address,
        action="append",
        default=[],
        help=f"connect to an upstream {server_name}; may be given multiple times",
    )
    parser.add_argument(
        "--listen",
        metavar="ip:port",
        type=parse_address,
        action="append",
        default=[],
        help="accept downstream connections on ip:port; may be given multiple times",
    )
    add_debug_argument(parser)
    return parser


async def run_server(
    node: Any, upstreams: List[Tuple[str, int]], listens: List[Tuple[str, int]]
) -> None:
    """Connect `node` upstream, start listening, and run until cancelled.

    Raises ConnectFailed / ListenFailed if an address can't be used.
    """
    try:
        for host, port in upstreams:
            try:
                await node.establish_connection(host, port)
            except OSError as exc:
                raise ConnectFailed(host, port, exc) from exc
        for host, port in listens:
            try:
                await node.accept_connection(host, port)
            except OSError as exc:
                raise ListenFailed(host, port, exc) from exc

        await asyncio.Event().wait()
    finally:
        await node.close()


async def repeat(
    action: Callable[[], Awaitable[Any]], count: int, interval: float
) -> None:
    """Await `action()` `count` times, sleeping `interval` seconds in between."""
    for i in range(count):
        if i > 0:
            await asyncio.sleep(interval)
        await action()


def print_received(text: str, time_stamp: bool) -> None:
    if time_stamp:
        text = f"[{datetime.now().isoformat()}] {text}"
    print(text, flush=True)


class ScriptError(Exception):
    """A failure a script reports as a one-line error message and exit status 1."""


class ConnectFailed(ScriptError):
    """A client script could not connect to its server."""

    def __init__(self, host: str, port: int, error: OSError) -> None:
        super().__init__(f"cannot connect to {host}:{port} ({error})")
        self.error = error


class ListenFailed(ScriptError):
    """A server script could not listen on an address (e.g. it is in use)."""

    def __init__(self, host: str, port: int, error: OSError) -> None:
        super().__init__(f"cannot listen on {host}:{port} ({error})")
        self.error = error


class ConnectionLost(ScriptError):
    """A client script's connection to its server was lost."""

    def __init__(self, error: Optional[BaseException]) -> None:
        super().__init__(f"connection to server lost ({error})")
        self.error = error


async def connect(client: Any, host: str, port: int) -> None:
    """Connect `client` (a BaseLayerClient or PubSubClient), raising
    ConnectFailed instead of a bare OSError if the server can't be reached."""
    try:
        await client.connect(host, port)
    except OSError as exc:
        raise ConnectFailed(host, port, exc) from exc


async def run_until_connection_lost(client: Any, body: Awaitable[None]) -> None:
    """Run `body`, stopping it and raising ConnectionLost as soon as `client`
    (a BaseLayerClient or PubSubClient) reports its connection lost.

    The loss may happen at any point in `body`, e.g. partway through a
    `repeat` of sends, not only while it idles.
    """
    lost: asyncio.Future = asyncio.get_running_loop().create_future()

    def on_connection_error(connection: Any, error: Optional[BaseException]) -> None:
        if not lost.done():
            lost.set_result(error)

    client.register_connection_error_callback(on_connection_error)
    body_task = asyncio.ensure_future(body)
    try:
        await asyncio.wait({body_task, lost}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        if not body_task.done():
            body_task.cancel()
    if lost.done():
        # a send that failed with the loss re-raises into body; the loss is the story
        try:
            await body_task
        except BaseException:
            pass
        raise ConnectionLost(lost.result())
    await body_task


def run_until_interrupted(coro: Coroutine[Any, Any, None]) -> None:
    """Run a script's top-level coroutine, exiting quietly on Ctrl-C and with
    an error message and exit status 1 on a ScriptError (e.g. the server
    can't be reached, or the connection to it is lost)."""
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        pass
    except ScriptError as exc:
        sys.exit(f"error: {exc}")

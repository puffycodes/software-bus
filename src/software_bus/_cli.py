from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Coroutine, List, Tuple

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
    """Connect `node` upstream, start listening, and run until cancelled."""
    try:
        for host, port in upstreams:
            await node.establish_connection(host, port)
        for host, port in listens:
            await node.accept_connection(host, port)

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


def run_until_interrupted(coro: Coroutine[Any, Any, None]) -> None:
    """Run a script's top-level coroutine, exiting quietly on Ctrl-C."""
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        pass

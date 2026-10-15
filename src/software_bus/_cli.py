from __future__ import annotations

import argparse
import asyncio
import ipaddress
import logging
import random
import sys
from datetime import datetime
from typing import Any, Awaitable, Callable, Coroutine, Iterable, List, Optional, Tuple

from .base_layer import DEFAULT_HOST, DEFAULT_PORT
from .pubsub import CycleCheckRefused

logger = logging.getLogger(__name__)

# Reconnect delay (base-layer.md): doubles after each failed attempt, up to the
# maximum, times a random 0.5-1 so servers that lost the same upstream spread out.
FIRST_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0


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


def parse_timeout(value: str) -> float:
    """Parse a timeout in seconds: a number >= 0 (0 means wait forever)."""
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number of seconds, got {value!r}") from None
    if not seconds >= 0:  # also rejects nan
        raise argparse.ArgumentTypeError(f"expected a timeout >= 0, got {value!r}")
    return seconds


def add_debug_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug",
        type=parse_bool,
        default=False,
        metavar="true|false",
        help="print logging information (default: false)",
    )


def add_time_stamp_argument(parser: argparse.ArgumentParser, what: str) -> None:
    """Add --time-stamp; `what` is e.g. "received message"."""
    parser.add_argument(
        "--time-stamp",
        type=parse_bool,
        default=True,
        metavar="true|false",
        help=f"print a time stamp with each {what} (default: true)",
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
    parser.add_argument(
        "--reconnect",
        type=parse_bool,
        default=False,
        metavar="true|false",
        help="keep trying an upstream that can't be reached or is lost (default: false)",
    )
    add_debug_argument(parser)
    return parser


async def run_server(
    node: Any,
    upstreams: List[Tuple[str, int]],
    listens: List[Tuple[str, int]],
    reconnect: bool = False,
) -> None:
    """Connect `node` upstream, start listening, and run until cancelled.

    Raises ListenFailed if a listen address can't be used. Without `reconnect`,
    raises ConnectFailed if an upstream can't be connected to, and never
    connects to a lost upstream again. With it, see `_keep_connected`.
    """
    upstream_tasks: List["asyncio.Future[None]"] = []
    try:
        if not reconnect:
            for host, port in upstreams:
                try:
                    await node.establish_connection(host, port)
                except OSError as exc:
                    raise ConnectFailed(host, port, exc) from exc
        await _listen(node, listens)  # with reconnect, without waiting for upstreams
        if reconnect:
            upstream_tasks = [
                asyncio.ensure_future(_keep_connected(node, host, port))
                for host, port in upstreams
            ]
            await asyncio.gather(*upstream_tasks)  # raises if one fails at start-up
        await asyncio.Event().wait()
    finally:
        for task in upstream_tasks:
            task.cancel()
        await asyncio.gather(*upstream_tasks, return_exceptions=True)
        await node.close()


async def _listen(node: Any, listens: List[Tuple[str, int]]) -> None:
    for host, port in listens:
        try:
            await node.accept_connection(host, port)
        except OSError as exc:
            raise ListenFailed(host, port, exc) from exc
        if reachable_from_other_machines(host):
            # allowed (e.g. a trusted private network), but the bus has no authentication
            logger.warning(
                "Listening on %s:%s, which other machines can reach: anyone who can "
                "reach it can use the bus, which has no authentication",
                host,
                port,
            )


def reachable_from_other_machines(host: str) -> bool:
    """False only for `localhost` and loopback IP addresses (Trust Model in base-layer.md).

    Any other host name counts as reachable: telling would mean resolving it.
    """
    if host == "localhost":
        return False
    try:
        return not ipaddress.ip_address(host).is_loopback
    except ValueError:
        return True


async def _keep_connected(
    node: Any,
    host: str,
    port: int,
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
) -> None:
    """Connect `node` to one upstream, and again whenever that fails or is lost.

    Waits the reconnect delay between attempts. A cycle check refusal that
    can't go away (anything but "could not determine") ends the attempts:
    on the first attempt by raising ConnectFailed, so the server exits as it
    would without --reconnect; later by logging it and returning, so the
    server keeps serving its other connections. `sleep` is replaceable for tests.
    """
    delay = FIRST_RECONNECT_DELAY
    first_attempt = True
    while True:
        try:
            connection = await node.establish_connection(host, port)
        except CycleCheckRefused as exc:
            if not exc.retryable:
                if first_attempt:
                    raise ConnectFailed(host, port, exc) from exc
                logger.error("Not reconnecting to %s:%s: %s", host, port, exc)
                return
            logger.warning("Cannot connect to %s:%s (%s); trying again", host, port, exc)
        except OSError as exc:
            logger.warning("Cannot connect to %s:%s (%s); trying again", host, port, exc)
        else:
            logger.info("Connected to upstream %s:%s", host, port)
            delay = FIRST_RECONNECT_DELAY
            await connection.wait_ended()
            logger.warning("Lost the connection to upstream %s:%s; reconnecting", host, port)
        first_attempt = False
        await sleep(delay * random.uniform(0.5, 1))
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


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


class InvalidSubject(ScriptError):
    """A subject given to a script can't be used (e.g. it contains a wild card)."""

    def __init__(self, subject: str, error: ValueError) -> None:
        super().__init__(f"invalid subject {subject!r} ({error})")
        self.error = error


class NoReply(ScriptError):
    """A request's wait ended by its timeout without any reply."""

    def __init__(self, subject: str) -> None:
        super().__init__(f"no reply to {subject!r} before the timeout")


def check_subjects(check: Callable[[str], None], subjects: Iterable[str]) -> None:
    """Run `check` (e.g. pubsub.check_subject) on each subject, raising
    InvalidSubject for the first it rejects with a ValueError.

    Scripts call this before connecting, so an invalid subject is reported
    even with no server.
    """
    for subject in subjects:
        try:
            check(subject)
        except ValueError as exc:
            raise InvalidSubject(subject, exc) from exc


async def connect(client: Any, host: str, port: int) -> None:
    """Connect `client` (a BaseLayerClient, PubSubClient or RequestReplyClient), raising
    ConnectFailed instead of a bare OSError if the server can't be reached."""
    try:
        await client.connect(host, port)
    except OSError as exc:
        raise ConnectFailed(host, port, exc) from exc


async def run_until_connection_lost(client: Any, body: Awaitable[None]) -> None:
    """Run `body`, stopping it and raising ConnectionLost as soon as `client`
    (a BaseLayerClient, PubSubClient or RequestReplyClient) reports its connection lost.

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

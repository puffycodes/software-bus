"""rr_request: send a request on a Request/Reply bus and print the replies from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from ._cli import (
    InvalidSubject,
    NoReply,
    add_debug_argument,
    add_upstream_argument,
    configure_logging,
    connect,
    parse_bool,
    parse_timeout,
    print_received,
    run_until_connection_lost,
    run_until_interrupted,
)
from .request_reply import DEFAULT_TIMEOUT, ReplyStatus, RequestReplyClient, check_request_subject


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a request on a Request/Reply bus and print the replies."
    )
    add_upstream_argument(parser, "ps_server")
    parser.add_argument(
        "--request-subject",
        required=True,
        help="request subject to send the request to",
    )
    parser.add_argument(
        "--request-message",
        default="",
        help="message to send with the request (default: empty)",
    )
    parser.add_argument(
        "--timeout",
        type=parse_timeout,
        default=DEFAULT_TIMEOUT,
        metavar="n",
        help=f"seconds to wait for replies; 0 waits forever (default: {DEFAULT_TIMEOUT:g})",
    )
    parser.add_argument(
        "--continue-waiting",
        type=parse_bool,
        default=True,
        metavar="true|false",
        help="keep waiting for more replies until the timeout; "
        "false stops after the first reply (default: true)",
    )
    parser.add_argument(
        "--time-stamp",
        type=parse_bool,
        default=True,
        metavar="true|false",
        help="print a time stamp with each reply and with a timeout (default: true)",
    )
    add_debug_argument(parser)
    return parser


async def run(
    host: str,
    port: int,
    request_subject: str,
    request_message: str = "",
    timeout: float = DEFAULT_TIMEOUT,
    continue_waiting: bool = True,
    time_stamp: bool = True,
) -> None:
    """Send the request and print replies until the wait ends.

    Raises NoReply if the timeout ends the wait before any reply arrived.
    """
    # checked before connecting: an invalid subject is reported even with no server
    try:
        check_request_subject(request_subject, sending=True)
    except ValueError as exc:
        raise InvalidSubject(request_subject, exc) from exc

    client = RequestReplyClient()
    ended: asyncio.Future = asyncio.get_running_loop().create_future()
    replies = 0

    def on_reply(reply_subject: str, payload: bytes, status: ReplyStatus) -> bool:
        nonlocal replies
        if status is ReplyStatus.REPLY:
            replies += 1
            print_received(f"{request_subject}: {payload.decode(errors='replace')}", time_stamp)
            if continue_waiting:
                return False
        elif status is ReplyStatus.TIMED_OUT:
            print_received(f"{request_subject}: timed out", time_stamp)
        else:
            # CONNECTION_LOST is reported by run_until_connection_lost as an error;
            # CLOSED only happens as we close the client ourselves
            return False
        if not ended.done():
            ended.set_result(None)
        return True

    async def body() -> None:
        await connect(client, host, port)
        await client.send_request(
            request_subject, request_message.encode(), on_reply, timeout
        )
        await ended

    try:
        await run_until_connection_lost(client, body())
    finally:
        await client.close()
    if replies == 0:
        raise NoReply(request_subject)


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    host, port = args.upstream
    run_until_interrupted(
        run(
            host,
            port,
            args.request_subject,
            args.request_message,
            args.timeout,
            args.continue_waiting,
            args.time_stamp,
        )
    )


if __name__ == "__main__":
    main()

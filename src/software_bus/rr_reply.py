"""rr_reply: accept requests on a Request/Reply bus and reply to them from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from ._cli import (
    InvalidSubject,
    add_debug_argument,
    add_upstream_argument,
    configure_logging,
    connect,
    parse_bool,
    parse_subject_list,
    print_received,
    run_until_connection_lost,
    run_until_interrupted,
)
from .request_reply import RequestReplyClient, check_request_subject


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Accept requests on a Request/Reply bus and reply to them."
    )
    add_upstream_argument(parser, "ps_server")
    parser.add_argument(
        "--accept-request",
        type=parse_subject_list,
        required=True,
        metavar='"<request_subject_1>,<request_subject_2>,..."',
        help="comma-separated request subjects to accept requests on",
    )
    parser.add_argument(
        "--reply-message",
        required=True,
        help='message to reply with; the reply is "Reply for <request_subject>: <reply_message>"',
    )
    parser.add_argument(
        "--time-stamp",
        type=parse_bool,
        default=True,
        metavar="true|false",
        help="print a time stamp with each received request (default: true)",
    )
    add_debug_argument(parser)
    return parser


async def run(
    host: str,
    port: int,
    request_subjects: List[str],
    reply_message: str,
    time_stamp: bool = True,
) -> None:
    # checked before connecting: an invalid subject is reported even with no server
    for request_subject in request_subjects:
        try:
            check_request_subject(request_subject)
        except ValueError as exc:
            raise InvalidSubject(request_subject, exc) from exc

    client = RequestReplyClient()

    async def on_request(request_subject: str, reply_subject: str, payload: bytes) -> None:
        print_received(f"{request_subject}: {payload.decode(errors='replace')}", time_stamp)
        await client.send_reply(
            reply_subject, f"Reply for {request_subject}: {reply_message}".encode()
        )

    async def body() -> None:
        await connect(client, host, port)
        for request_subject in dict.fromkeys(request_subjects):  # duplicates omitted
            await client.accept_request(request_subject, on_request)
        await asyncio.Event().wait()

    try:
        await run_until_connection_lost(client, body())
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    host, port = args.upstream
    run_until_interrupted(
        run(host, port, args.accept_request, args.reply_message, args.time_stamp)
    )


if __name__ == "__main__":
    main()

"""ps_subscribe: subscribe to subjects on a Publish/Subscribe bus from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from ._cli import (
    add_debug_argument,
    add_upstream_argument,
    configure_logging,
    parse_bool,
    parse_subject_list,
    print_received,
    run_until_interrupted,
)
from .pubsub import PubSubClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Subscribe to subjects on a Publish/Subscribe bus."
    )
    add_upstream_argument(parser, "ps_server")
    parser.add_argument(
        "--subject",
        type=parse_subject_list,
        required=True,
        metavar='"<subject_1>,<subject_2>,..."',
        help="comma-separated subjects to subscribe to",
    )
    parser.add_argument(
        "--time-stamp",
        type=parse_bool,
        default=True,
        metavar="true|false",
        help="print a time stamp with each received publish (default: true)",
    )
    add_debug_argument(parser)
    return parser


async def run(
    host: str,
    port: int,
    subjects: List[str],
    time_stamp: bool = True,
) -> None:
    def on_publish(matched_subject: str, actual_subject: str, payload: bytes) -> None:
        print_received(
            f"{matched_subject} {actual_subject}: {payload.decode(errors='replace')}",
            time_stamp,
        )

    client = PubSubClient()
    try:
        await client.connect(host, port)
        for subject in subjects:
            await client.subscribe(subject, on_publish)
        await asyncio.Event().wait()
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    host, port = args.upstream
    run_until_interrupted(run(host, port, args.subject, args.time_stamp))


if __name__ == "__main__":
    main()

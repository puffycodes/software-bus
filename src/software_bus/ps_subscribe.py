"""ps_subscribe: subscribe to subjects on a Publish/Subscribe bus from the command line."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from typing import List, Optional

from ._cli import parse_address, parse_bool, parse_subject_list
from .base_layer import DEFAULT_HOST, DEFAULT_PORT
from .pubsub import PubSubClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Subscribe to subjects on a Publish/Subscribe bus."
    )
    parser.add_argument(
        "--upstream",
        metavar="ip:port",
        type=parse_address,
        default=(DEFAULT_HOST, DEFAULT_PORT),
        help=f"ps_server to connect to (default: {DEFAULT_HOST}:{DEFAULT_PORT})",
    )
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
    return parser


def _print_received(subject: str, payload: bytes, time_stamp: bool) -> None:
    text = f"{subject}: {payload.decode(errors='replace')}"
    if time_stamp:
        text = f"[{datetime.now().isoformat()}] {text}"
    print(text, flush=True)


async def run(
    host: str,
    port: int,
    subjects: List[str],
    time_stamp: bool = True,
) -> None:
    client = PubSubClient()
    client.register_publish_callback(
        lambda subject, payload: _print_received(subject, payload, time_stamp)
    )
    try:
        await client.connect(host, port)
        for subject in subjects:
            await client.subscribe(subject)
        await asyncio.Event().wait()
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    host, port = args.upstream
    try:
        asyncio.run(run(host, port, args.subject, args.time_stamp))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

"""ps_publish: publish messages on a Publish/Subscribe bus from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from ._cli import configure_logging, parse_address, parse_bool
from .base_layer import DEFAULT_HOST, DEFAULT_PORT
from .pubsub import PubSubClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish messages on a Publish/Subscribe bus."
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
        required=True,
        help="subject to publish to",
    )
    parser.add_argument(
        "--message",
        required=True,
        help="message to publish",
    )
    parser.add_argument(
        "--repeat-count",
        type=int,
        default=1,
        metavar="n",
        help="number of times to publish the message (default: 1)",
    )
    parser.add_argument(
        "--repeat-interval",
        type=float,
        default=1.0,
        metavar="t",
        help="seconds to wait between repeated publishes (default: 1)",
    )
    parser.add_argument(
        "--debug",
        type=parse_bool,
        default=False,
        metavar="true|false",
        help="print logging information (default: false)",
    )
    return parser


async def run(
    host: str,
    port: int,
    subject: str,
    message: str,
    repeat_count: int = 1,
    repeat_interval: float = 1.0,
) -> None:
    client = PubSubClient()
    try:
        await client.connect(host, port)
        payload = message.encode()
        for i in range(repeat_count):
            if i > 0:
                await asyncio.sleep(repeat_interval)
            await client.publish(subject, payload)
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    host, port = args.upstream
    try:
        asyncio.run(
            run(
                host,
                port,
                args.subject,
                args.message,
                args.repeat_count,
                args.repeat_interval,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

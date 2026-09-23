"""ps_publish: publish messages on a Publish/Subscribe bus from the command line."""
from __future__ import annotations

import argparse
from typing import List, Optional

from ._cli import (
    add_debug_argument,
    add_repeat_arguments,
    add_upstream_argument,
    configure_logging,
    repeat,
    run_until_interrupted,
)
from .pubsub import PubSubClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish messages on a Publish/Subscribe bus."
    )
    add_upstream_argument(parser, "ps_server")
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
    add_repeat_arguments(parser, "publish")
    add_debug_argument(parser)
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
        await repeat(lambda: client.publish(subject, payload), repeat_count, repeat_interval)
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    host, port = args.upstream
    run_until_interrupted(
        run(
            host,
            port,
            args.subject,
            args.message,
            args.repeat_count,
            args.repeat_interval,
        )
    )


if __name__ == "__main__":
    main()

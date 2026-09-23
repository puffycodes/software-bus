"""bl_client: send messages through a Base Layer bus from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional, Union

from ._cli import (
    add_debug_argument,
    add_repeat_arguments,
    add_upstream_argument,
    configure_logging,
    parse_bool,
    print_received,
    repeat,
    run_until_interrupted,
)
from .client import BaseLayerClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send messages through a Base Layer bus."
    )
    add_upstream_argument(parser, "bl_server")
    parser.add_argument(
        "--message",
        default=None,
        help="message to send (default: None, don't send any message)",
    )
    add_repeat_arguments(parser, "send")
    parser.add_argument(
        "--time-stamp",
        type=parse_bool,
        default=True,
        metavar="true|false",
        help="print a time stamp with each received message (default: true)",
    )
    add_debug_argument(parser)
    return parser


async def run(
    host: str,
    port: int,
    message: Union[str, None],
    repeat_count: int = 1,
    repeat_interval: float = 1.0,
    time_stamp: bool = True,
) -> None:
    client = BaseLayerClient()
    client.register_receive_callback(
        lambda data: print_received(data.decode(errors="replace"), time_stamp)
    )
    try:
        await client.connect(host, port)
        if message is not None:
            data = message.encode()
            await repeat(lambda: client.send(data), repeat_count, repeat_interval)
        await asyncio.Event().wait()
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
            args.message,
            args.repeat_count,
            args.repeat_interval,
            args.time_stamp,
        )
    )


if __name__ == "__main__":
    main()

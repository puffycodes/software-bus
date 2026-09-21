"""bl_client: send messages through a Base Layer bus from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional, Union

from ._cli import parse_address
from .base_layer import DEFAULT_HOST, DEFAULT_PORT
from .client import BaseLayerClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send messages through a Base Layer bus."
    )
    parser.add_argument(
        "--upstream",
        metavar="ip:port",
        type=parse_address,
        default=(DEFAULT_HOST, DEFAULT_PORT),
        help=f"bl_server to connect to (default: {DEFAULT_HOST}:{DEFAULT_PORT})",
    )
    parser.add_argument(
        "--message",
        default=None,
        help="message to send (default: None, don't send any message)",
    )
    parser.add_argument(
        "--repeat-count",
        type=int,
        default=1,
        metavar="n",
        help="number of times to send the message (default: 1)",
    )
    parser.add_argument(
        "--repeat-interval",
        type=float,
        default=1.0,
        metavar="t",
        help="seconds to wait between repeated sends (default: 1)",
    )
    return parser


async def run(
    host: str,
    port: int,
    message: Union[str, None],
    repeat_count: int = 1,
    repeat_interval: float = 1.0,
) -> None:
    client = BaseLayerClient()
    client.register_receive_callback(
        lambda data: print(data.decode(errors="replace"), flush=True)
    )
    try:
        await client.connect(host, port)
        if message is not None:
            data = message.encode()
            for i in range(repeat_count):
                if i > 0:
                    await asyncio.sleep(repeat_interval)
                await client.send(data)
        await asyncio.Event().wait()
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    host, port = args.upstream
    try:
        asyncio.run(
            run(
                host,
                port,
                args.message,
                args.repeat_count,
                args.repeat_interval,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

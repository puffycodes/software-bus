"""bl_client: send a message through a Base Layer bus from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from ._cli import parse_address
from .base_layer import DEFAULT_HOST, DEFAULT_PORT
from .client import BaseLayerClient


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a message through a Base Layer bus."
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
        required=True,
        help="message to send",
    )
    return parser


async def run(host: str, port: int, message: str) -> None:
    client = BaseLayerClient()
    client.register_receive_callback(
        lambda data: print(data.decode(errors="replace"), flush=True)
    )
    try:
        await client.connect(host, port)
        await client.send(message.encode())
        await asyncio.Event().wait()
    finally:
        await client.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    host, port = args.upstream
    try:
        asyncio.run(run(host, port, args.message))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

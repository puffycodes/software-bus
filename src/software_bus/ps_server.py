"""ps_server: run a Publish/Subscribe bus node from the command line."""
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional, Tuple

from ._cli import parse_address
from .pubsub import PubSubNode


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Publish/Subscribe bus node.")
    parser.add_argument(
        "--upstream",
        metavar="ip:port",
        type=parse_address,
        action="append",
        default=[],
        help="connect to an upstream ps_server; may be given multiple times",
    )
    parser.add_argument(
        "--listen",
        metavar="ip:port",
        type=parse_address,
        action="append",
        default=[],
        help="accept downstream connections on ip:port; may be given multiple times",
    )
    return parser


async def run(
    upstreams: List[Tuple[str, int]], listens: List[Tuple[str, int]]
) -> None:
    node = PubSubNode()
    try:
        for host, port in upstreams:
            await node.establish_connection(host, port)
        for host, port in listens:
            await node.accept_connection(host, port)

        await asyncio.Event().wait()
    finally:
        await node.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    try:
        asyncio.run(run(args.upstream, args.listen))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

"""bl_server: run a Base Layer bus node from the command line."""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import List, Optional, Tuple

from ._cli import parse_address, parse_bool
from .base_layer import BaseLayerNode


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Base Layer bus node.")
    parser.add_argument(
        "--upstream",
        metavar="ip:port",
        type=parse_address,
        action="append",
        default=[],
        help="connect to an upstream bl_server; may be given multiple times",
    )
    parser.add_argument(
        "--listen",
        metavar="ip:port",
        type=parse_address,
        action="append",
        default=[],
        help="accept downstream connections on ip:port; may be given multiple times",
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
    upstreams: List[Tuple[str, int]], listens: List[Tuple[str, int]]
) -> None:
    layer = BaseLayerNode()
    try:
        for host, port in upstreams:
            await layer.establish_connection(host, port)
        for host, port in listens:
            await layer.accept_connection(host, port)

        await asyncio.Event().wait()
    finally:
        await layer.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    if args.debug:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    try:
        asyncio.run(run(args.upstream, args.listen))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

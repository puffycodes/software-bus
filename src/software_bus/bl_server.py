"""bl_server: run a Base Layer bus node from the command line."""
from __future__ import annotations

import argparse
from typing import List, Optional, Tuple

from ._cli import build_server_arg_parser, configure_logging, run_server, run_until_interrupted
from .base_layer import BaseLayerNode


def build_arg_parser() -> argparse.ArgumentParser:
    return build_server_arg_parser("Run a Base Layer bus node.", "bl_server")


async def run(
    upstreams: List[Tuple[str, int]], listens: List[Tuple[str, int]]
) -> None:
    await run_server(BaseLayerNode(), upstreams, listens)


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    run_until_interrupted(run(args.upstream, args.listen))


if __name__ == "__main__":
    main()

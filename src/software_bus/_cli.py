from __future__ import annotations

import argparse
from typing import Tuple


def parse_address(value: str) -> Tuple[str, int]:
    """Parse an "ip:port" command-line argument."""
    host, sep, port = value.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected ip:port, got {value!r}")
    return host, int(port)

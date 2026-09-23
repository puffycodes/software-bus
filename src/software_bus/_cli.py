from __future__ import annotations

import argparse
import logging
from typing import List, Tuple


def configure_logging(debug: bool) -> None:
    """Enable INFO-level logging output when --debug is set."""
    if debug:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )


def parse_address(value: str) -> Tuple[str, int]:
    """Parse an "ip:port" command-line argument."""
    host, sep, port = value.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected ip:port, got {value!r}")
    return host, int(port)


def parse_bool(value: str) -> bool:
    """Parse a "true"/"false" command-line argument."""
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise argparse.ArgumentTypeError(f"expected true|false, got {value!r}")


def parse_subject_list(value: str) -> List[str]:
    """Parse a comma-separated "<subject_1>,<subject_2>,..." argument."""
    subjects = [subject.strip() for subject in value.split(",")]
    subjects = [subject for subject in subjects if subject]
    if not subjects:
        raise argparse.ArgumentTypeError(f"expected one or more subjects, got {value!r}")
    return subjects

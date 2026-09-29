"""bus_check: find cycles among the bus nodes running on this machine.

Looks at the nodes' TCP connections as the operating system reports them
(via psutil), so it sends nothing over the bus; see docs/design/bus-check.md.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ._cli import add_debug_argument, configure_logging

Address = Tuple[str, int]

EXIT_OK = 0
EXIT_CYCLES = 1
EXIT_ERROR = 2

_NODE_KINDS = ("bl_server", "ps_server")


class CheckError(Exception):
    """The check could not be run (e.g. psutil missing, or access refused)."""


@dataclass
class Node:
    pid: int
    kind: str  # "bl_server", "ps_server", or "node" for one given with --pid
    listening: List[Address] = field(default_factory=list)

    def describe(self) -> str:
        where = ", ".join(f"{host}:{port}" for host, port in self.listening) or "not listening"
        return f"pid {self.pid} ({self.kind}, {where})"


@dataclass
class TcpConnection:
    """One end of a TCP connection, as seen by the process `pid` (None if hidden)."""

    pid: Optional[int]
    local: Address
    remote: Optional[Address]  # None for a listening socket
    listening: bool = False


@dataclass
class Report:
    nodes: Dict[int, Node]
    links: List[Tuple[int, int]]
    cycles: List[List[int]]
    unchecked_remote: int = 0  # node connections to other machines (or hidden processes)
    hidden: int = 0  # connections whose owning process the OS didn't reveal


def _basename_stem(path: str) -> str:
    name = os.path.basename(path.replace("\\", "/")).lower()
    for suffix in (".exe", ".py"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name


def node_kind(cmdline: Sequence[str]) -> Optional[str]:
    """"bl_server"/"ps_server" if `cmdline` runs that script, else None.

    Matches `bl_server ...` (the installed command, possibly run as
    `python /path/to/bl_server`) and `python -m software_bus.bl_server ...`.
    """
    if not cmdline:
        return None
    program = _basename_stem(cmdline[0])
    if program in _NODE_KINDS:
        return program
    if not (program.startswith("python") or program == "py"):
        return None
    args = list(cmdline[1:])
    if "-m" in args:
        module = args[args.index("-m") + 1] if args.index("-m") + 1 < len(args) else ""
        for kind in _NODE_KINDS:
            if module == f"software_bus.{kind}":
                return kind
        return None
    # the first non-option argument is the script python runs
    script = next((a for a in args if not a.startswith("-")), None)
    if script is not None and _basename_stem(script) in _NODE_KINDS:
        return _basename_stem(script)
    return None


_WILDCARD_HOSTS = ("0.0.0.0", "::", "")


def _listens_on(node: Node, address: Address) -> bool:
    host, port = address
    return any(
        port == listen_port and (host == listen_host or listen_host in _WILDCARD_HOSTS)
        for listen_host, listen_port in node.listening
    )


def find_links(
    connections: Iterable[TcpConnection], nodes: Dict[int, Node]
) -> Tuple[List[Tuple[int, int]], int, int]:
    """Pair up the two ends of every connection between nodes.

    Returns (links, unchecked_remote, hidden): each link is (pid, pid) and
    appears once per TCP connection; `unchecked_remote` counts node
    connections whose other end isn't on this machine (or can't be seen);
    `hidden` counts connections the OS reported without an owning process.

    A connection only counts as a link if one end is an address a node
    listens on, as every connection made by `establish_connection` is. That
    leaves out loopback connections a process makes to itself for its own
    use (asyncio does this on Windows), which aren't bus links.
    """
    connections = [c for c in connections if not c.listening and c.remote is not None]
    all_ends = {(c.local, c.remote) for c in connections}
    node_ends: Dict[Tuple[Address, Address], int] = {
        (c.local, c.remote): c.pid for c in connections if c.pid in nodes
    }
    links = []
    unchecked_remote = 0
    for (local, remote), pid in node_ends.items():
        other_pid = node_ends.get((remote, local))
        if other_pid is not None:
            is_bus_link = _listens_on(nodes[pid], local) or _listens_on(nodes[other_pid], remote)
            if is_bus_link and (local, remote) < (remote, local):  # count each connection once
                links.append((pid, other_pid))
        elif (remote, local) not in all_ends:
            unchecked_remote += 1
    hidden = sum(1 for c in connections if c.pid is None)
    return links, unchecked_remote, hidden


def find_cycles(pids: Iterable[int], links: Iterable[Tuple[int, int]]) -> List[List[int]]:
    """One cycle per link that joins two already-connected nodes.

    Each cycle is the list of pids along it (the link back to the first pid
    is implied): [a] for a node connected to itself, [a, b] for two links
    between the same pair, and so on.
    """
    parent = {pid: pid for pid in pids}

    def root(pid: int) -> int:
        while parent[pid] != pid:
            parent[pid] = parent[parent[pid]]
            pid = parent[pid]
        return pid

    tree: Dict[int, List[int]] = defaultdict(list)
    cycles = []
    for a, b in links:
        if a == b:
            cycles.append([a])
        elif root(a) == root(b):
            cycles.append(_tree_path(tree, a, b))
        else:
            parent[root(a)] = root(b)
            tree[a].append(b)
            tree[b].append(a)
    return cycles


def _tree_path(tree: Dict[int, List[int]], start: int, end: int) -> List[int]:
    came_from = {start: start}
    queue = deque([start])
    while queue:
        pid = queue.popleft()
        if pid == end:
            break
        for neighbour in tree[pid]:
            if neighbour not in came_from:
                came_from[neighbour] = pid
                queue.append(neighbour)
    path = [end]
    while path[-1] != start:
        path.append(came_from[path[-1]])
    return path[::-1]


def check(nodes: Dict[int, Node], connections: List[TcpConnection]) -> Report:
    for c in connections:
        if c.listening and c.pid in nodes and c.local not in nodes[c.pid].listening:
            nodes[c.pid].listening.append(c.local)
    links, unchecked_remote, hidden = find_links(connections, nodes)
    return Report(
        nodes=nodes,
        links=links,
        cycles=find_cycles(nodes, links),
        unchecked_remote=unchecked_remote,
        hidden=hidden,
    )


def collect(extra_pids: Sequence[int] = ()) -> Tuple[Dict[int, Node], List[TcpConnection]]:
    """The nodes running on this machine and all TCP connections, via psutil."""
    try:
        import psutil
    except ImportError:
        raise CheckError(
            'bus_check needs psutil: pip install "software-bus[check]" (or: pip install psutil)'
        ) from None

    nodes: Dict[int, Node] = {}
    for proc in psutil.process_iter(["pid", "cmdline"]):
        kind = node_kind(proc.info.get("cmdline") or [])
        if kind is not None:
            nodes[proc.info["pid"]] = Node(proc.info["pid"], kind)
    for pid in extra_pids:
        nodes.setdefault(pid, Node(pid, "node"))

    try:
        raw = psutil.net_connections(kind="tcp")
    except psutil.AccessDenied:
        raise CheckError(
            "the operating system refused to list connections; try again as administrator/root"
        ) from None
    connections = []
    for c in raw:
        listening = c.status == psutil.CONN_LISTEN
        if not listening and c.status != psutil.CONN_ESTABLISHED:
            continue
        connections.append(
            TcpConnection(
                pid=c.pid,
                local=(c.laddr.ip, c.laddr.port),
                remote=(c.raddr.ip, c.raddr.port) if c.raddr else None,
                listening=listening,
            )
        )
    return nodes, connections


def format_report(report: Report) -> str:
    lines = [f"Nodes found: {len(report.nodes)}"]
    for pid in sorted(report.nodes):
        lines.append(f"  {report.nodes[pid].describe()}")
    lines.append(f"Links between them: {len(report.links)}")
    if report.unchecked_remote:
        lines.append(
            f"warning: {report.unchecked_remote} node connection(s) go to other machines "
            "(or to processes this check can't see); those are not checked"
        )
    if report.hidden:
        lines.append(
            f"warning: the owning process of {report.hidden} connection(s) is hidden; "
            "the check may be incomplete (try again as administrator/root)"
        )
    if not report.cycles:
        lines.append("No cycles found.")
        return "\n".join(lines)
    lines.append(f"Cycles found: {len(report.cycles)}")
    for cycle in report.cycles:
        first = report.nodes[cycle[0]]
        if len(cycle) == 1:
            lines.append(f"  {first.describe()} is connected to itself")
            continue
        path = " - ".join(report.nodes[pid].describe() for pid in cycle)
        lines.append(f"  {path} - back to pid {first.pid}")
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find cycles among the bus nodes (bl_server/ps_server) running on this machine."
    )
    parser.add_argument(
        "--pid",
        type=int,
        action="append",
        default=[],
        metavar="PID",
        help="also treat process PID as a node; may be repeated (default: None)",
    )
    add_debug_argument(parser)
    return parser


def run(extra_pids: Sequence[int] = ()) -> int:
    """Run the check and print its report; return the exit status."""
    try:
        nodes, connections = collect(extra_pids)
    except CheckError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    report = check(nodes, connections)
    print(format_report(report))
    return EXIT_CYCLES if report.cycles else EXIT_OK


def main(argv: Optional[List[str]] = None) -> None:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.debug)
    sys.exit(run(args.pid))


if __name__ == "__main__":
    main()

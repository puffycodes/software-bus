import os
import socket
import subprocess
import sys
import time

import pytest

from software_bus import bus_check
from software_bus.bus_check import (
    CheckError,
    Node,
    TcpConnection,
    check,
    drop_launchers,
    find_cycles,
    find_links,
    format_report,
    node_kind,
)

from helpers import free_port


# --- recognising node processes ------------------------------------------------


@pytest.mark.parametrize(
    "cmdline, kind",
    [
        (["python", "-m", "software_bus.bl_server", "--listen", "127.0.0.1:8787"], "bl_server"),
        (["/usr/bin/python3.8", "-m", "software_bus.ps_server"], "ps_server"),
        ([r"C:\venv\Scripts\python.exe", "-m", "software_bus.ps_server"], "ps_server"),
        ([r"C:\venv\Scripts\bl_server.exe", "--listen", "127.0.0.1:8787"], "bl_server"),
        (["python3", "/home/user/.local/bin/ps_server", "--listen", "127.0.0.1:8787"], "ps_server"),
        (["python3", "-u", "/home/user/.local/bin/bl_server"], "bl_server"),
        (["python", "-m", "software_bus.bl_client"], None),  # a client, not a node
        (["python", "-m", "pytest"], None),
        (["grep", "bl_server"], None),  # mentions it, but doesn't run it
        (["python", "-c", "print('bl_server')"], None),
        ([], None),
    ],
)
def test_node_kind(cmdline, kind):
    assert node_kind(cmdline) == kind


def test_drop_launchers_keeps_only_the_python_child_of_a_windows_launcher():
    # bl_server.exe (pid 10) runs `python.exe ...l_server.exe` (pid 11)
    nodes = {10: Node(10, "bl_server"), 11: Node(11, "bl_server"), 20: Node(20, "ps_server")}
    parents = {10: 1, 11: 10, 20: 1}
    assert sorted(drop_launchers(nodes, parents)) == [11, 20]


def test_drop_launchers_keeps_a_node_started_by_a_node_of_another_kind():
    nodes = {10: Node(10, "bl_server"), 11: Node(11, "ps_server")}
    assert sorted(drop_launchers(nodes, {10: 1, 11: 10})) == [10, 11]


def test_drop_launchers_without_parents_keeps_every_node():
    nodes = {10: Node(10, "bl_server"), 11: Node(11, "bl_server")}
    assert sorted(drop_launchers(nodes, {})) == [10, 11]


# --- finding cycles --------------------------------------------------------------


def test_find_cycles_in_a_tree_finds_none():
    assert find_cycles([1, 2, 3, 4], [(2, 1), (3, 1), (4, 3)]) == []


def test_find_cycles_finds_a_triangle():
    (cycle,) = find_cycles([1, 2, 3], [(2, 1), (3, 1), (3, 2)])
    assert sorted(cycle) == [1, 2, 3]


def test_find_cycles_finds_two_links_between_the_same_nodes():
    assert find_cycles([1, 2], [(2, 1), (2, 1)]) == [[2, 1]]


def test_find_cycles_finds_a_node_connected_to_itself():
    assert find_cycles([1], [(1, 1)]) == [[1]]


def test_find_cycles_reports_each_cycle_path_in_order():
    # 1-2-3-4 is a chain; linking 4 back to 1 closes a cycle of four
    (cycle,) = find_cycles([1, 2, 3, 4], [(1, 2), (2, 3), (3, 4), (4, 1)])
    assert cycle == [4, 3, 2, 1]


def test_find_cycles_finds_separate_cycles():
    links = [(1, 2), (2, 3), (3, 1), (4, 5), (5, 4)]
    assert len(find_cycles([1, 2, 3, 4, 5], links)) == 2


# --- pairing connections into links -----------------------------------------------


def _node(pid, *listening):
    return Node(pid, "bl_server", list(listening))


def _connection(pid, local_port, remote_port, remote_host="127.0.0.1"):
    return TcpConnection(pid, ("127.0.0.1", local_port), (remote_host, remote_port))


def test_find_links_pairs_both_ends_of_a_connection_once():
    nodes = {1: _node(1, ("127.0.0.1", 8000)), 2: _node(2)}
    connections = [_connection(2, 50000, 8000), _connection(1, 8000, 50000)]
    links, unchecked, hidden = find_links(connections, nodes)
    assert len(links) == 1 and sorted(links[0]) == [1, 2]
    assert (unchecked, hidden) == (0, 0)


def test_find_links_ignores_clients():
    nodes = {1: _node(1, ("127.0.0.1", 8000))}
    connections = [_connection(99, 50000, 8000), _connection(1, 8000, 50000)]  # 99: a client
    assert find_links(connections, nodes) == ([], 0, 0)


def test_find_links_ignores_a_process_connected_to_itself_off_its_listening_port():
    # asyncio on Windows keeps a loopback connection to itself; it isn't a bus link
    nodes = {1: _node(1, ("127.0.0.1", 8000))}
    connections = [_connection(1, 60000, 60001), _connection(1, 60001, 60000)]
    assert find_links(connections, nodes)[0] == []


def test_find_links_keeps_a_node_connected_to_its_own_listening_port():
    nodes = {1: _node(1, ("127.0.0.1", 8000))}
    connections = [_connection(1, 50000, 8000), _connection(1, 8000, 50000)]
    assert find_links(connections, nodes)[0] == [(1, 1)]


def test_find_links_matches_a_wildcard_listening_address():
    nodes = {1: _node(1, ("0.0.0.0", 8000)), 2: _node(2)}
    connections = [_connection(2, 50000, 8000), _connection(1, 8000, 50000)]
    assert len(find_links(connections, nodes)[0]) == 1


def test_find_links_counts_connections_to_other_machines_and_hidden_processes():
    nodes = {1: _node(1, ("127.0.0.1", 8000))}
    connections = [
        _connection(1, 50000, 8787, remote_host="10.0.0.1"),  # other end not on this machine
        _connection(1, 8000, 50001),  # a node connection...
        _connection(None, 50001, 8000),  # ...whose other end's process the OS hid
    ]
    links, unchecked, hidden = find_links(connections, nodes)
    assert (links, unchecked, hidden) == ([], 1, 1)


def test_find_links_ignores_hidden_connections_no_node_takes_part_in():
    # e.g. another user's ssh session, as a normal user on Linux sees it
    nodes = {1: _node(1, ("127.0.0.1", 8000)), 2: _node(2)}
    connections = [
        _connection(2, 50000, 8000),
        _connection(1, 8000, 50000),
        TcpConnection(None, ("10.0.0.5", 22), ("10.0.0.9", 50123)),
    ]
    links, unchecked, hidden = find_links(connections, nodes)
    assert (len(links), unchecked, hidden) == (1, 0, 0)


# --- report and exit status -----------------------------------------------------------


def _triangle():
    nodes = {pid: Node(pid, "bl_server") for pid in (1, 2, 3)}
    connections = []
    for pid, port in ((1, 8001), (2, 8002), (3, 8003)):
        connections.append(TcpConnection(pid, ("127.0.0.1", port), None, listening=True))
    for client_pid, local_port, server_port, server_pid in (
        (2, 50001, 8001, 1),
        (3, 50002, 8001, 1),
        (3, 50003, 8002, 2),
    ):
        connections.append(_connection(client_pid, local_port, server_port))
        connections.append(_connection(server_pid, server_port, local_port))
    return nodes, connections


def test_check_fills_in_listening_addresses_and_finds_the_cycle():
    report = check(*_triangle())
    assert report.nodes[1].listening == [("127.0.0.1", 8001)]
    assert len(report.links) == 3
    assert len(report.cycles) == 1


def test_format_report_lists_nodes_and_the_cycle():
    text = format_report(check(*_triangle()))
    assert "Nodes found: 3" in text
    assert "pid 1 (bl_server, 127.0.0.1:8001)" in text
    assert "Cycles found: 1" in text
    assert "back to pid" in text


def test_check_reports_nodes_whose_sockets_are_hidden():
    # node 2's command line is visible but its sockets aren't (another user's process)
    nodes = {1: _node(1), 2: Node(2, "bl_server", sockets_hidden=True)}
    connections = [TcpConnection(1, ("127.0.0.1", 8001), None, listening=True)]
    report = check(nodes, connections)
    assert report.unseen == [2]
    assert "hides the sockets of node(s) 2" in format_report(report)


def test_check_does_not_warn_about_a_node_that_has_no_sockets():
    # e.g. started with neither --listen nor --upstream
    report = check({1: _node(1)}, [])
    assert report.unseen == []
    assert "warning" not in format_report(report)


class _FakePsutil:
    """Just enough of psutil for collect(): pid 2's sockets are hidden."""

    class AccessDenied(Exception):
        pass

    class NoSuchProcess(Exception):
        pass

    CONN_LISTEN = "LISTEN"
    CONN_ESTABLISHED = "ESTABLISHED"

    class _Proc:
        def __init__(self, pid, cmdline):
            self.info = {"pid": pid, "ppid": 1, "cmdline": cmdline}

    def process_iter(self, attrs):
        return [
            self._Proc(pid, ["python", "-m", "software_bus.bl_server"]) for pid in (2, 3)
        ]

    def Process(self, pid):
        fake = self

        class Process:
            def net_connections(self, kind):
                if pid == 2:
                    raise fake.AccessDenied()
                if pid == 99:
                    raise fake.NoSuchProcess()
                return []  # pid 3 simply has no sockets

        return Process()

    def net_connections(self, kind):
        return []


def test_collect_asks_the_os_whether_a_node_s_sockets_are_hidden(monkeypatch):
    monkeypatch.setitem(sys.modules, "psutil", _FakePsutil())
    nodes, _ = bus_check.collect(extra_pids=[99])  # 99: given with --pid, not running
    assert {pid: node.sockets_hidden for pid, node in nodes.items()} == {
        2: True,
        3: False,
        99: False,
    }


def test_format_report_warns_nothing_when_everything_is_seen():
    nodes, connections = _triangle()
    connections.append(TcpConnection(None, ("10.0.0.5", 22), ("10.0.0.9", 50123)))
    assert "warning" not in format_report(check(nodes, connections))


def test_format_report_without_cycles():
    nodes = {1: Node(1, "ps_server", [("127.0.0.1", 8001)])}
    text = format_report(check(nodes, []))
    assert "No cycles found." in text


def test_run_exit_status(monkeypatch, capsys):
    monkeypatch.setattr(bus_check, "collect", lambda extra_pids=(): _triangle())
    assert bus_check.run() == bus_check.EXIT_CYCLES

    monkeypatch.setattr(bus_check, "collect", lambda extra_pids=(): ({}, []))
    assert bus_check.run() == bus_check.EXIT_OK
    assert "No cycles found." in capsys.readouterr().out


def test_run_reports_a_check_error(monkeypatch, capsys):
    def fail(extra_pids=()):
        raise CheckError("psutil is missing")

    monkeypatch.setattr(bus_check, "collect", fail)
    assert bus_check.run() == bus_check.EXIT_ERROR
    assert "error: psutil is missing" in capsys.readouterr().err


def test_main_exits_with_run_status(monkeypatch):
    monkeypatch.setattr(bus_check, "collect", lambda extra_pids=(): _triangle())
    with pytest.raises(SystemExit) as exc_info:
        bus_check.main([])
    assert exc_info.value.code == bus_check.EXIT_CYCLES


def test_pid_option_accumulates():
    args = bus_check.build_arg_parser().parse_args(["--pid", "12", "--pid", "34"])
    assert args.pid == [12, 34]


# --- against real bl_server processes ------------------------------------------------------


def _start_bl_server(listen_port, *upstream_ports):
    argv = [sys.executable, "-m", "software_bus.bl_server", "--listen", f"127.0.0.1:{listen_port}"]
    for port in upstream_ports:
        argv += ["--upstream", f"127.0.0.1:{port}"]
    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
    env = {**os.environ, "PYTHONPATH": src + os.pathsep + os.environ.get("PYTHONPATH", "")}
    process = subprocess.Popen(argv, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:  # wait until it listens (so upstreams are connected too)
        try:
            socket.create_connection(("127.0.0.1", listen_port), timeout=0.2).close()
            return process
        except OSError:
            time.sleep(0.05)
    process.kill()
    raise RuntimeError("bl_server did not start")


def _check_only(pids):
    """Run the real check, limited to the given node processes (others may be running)."""
    nodes, connections = bus_check.collect()
    return check({pid: node for pid, node in nodes.items() if pid in pids}, connections)


def _wait_for_links(pids, count, timeout=5):
    deadline = time.monotonic() + timeout
    while True:
        report = _check_only(pids)
        if len(report.links) >= count or time.monotonic() > deadline:
            return report
        time.sleep(0.1)


@pytest.fixture
def servers():
    started = []
    yield started
    for process in started:
        process.kill()
        process.wait()


def test_real_servers_in_a_tree_have_no_cycle(servers):
    pytest.importorskip("psutil")
    a, b, c = free_port(), free_port(), free_port()
    servers.append(_start_bl_server(a))
    servers.append(_start_bl_server(b, a))
    servers.append(_start_bl_server(c, a))

    report = _wait_for_links({p.pid for p in servers}, 2)
    assert {node.kind for node in report.nodes.values()} == {"bl_server"}
    assert len(report.nodes) == 3
    assert len(report.links) == 2
    assert report.cycles == []


def test_real_servers_in_a_triangle_have_a_cycle(servers):
    pytest.importorskip("psutil")
    a, b, c = free_port(), free_port(), free_port()
    servers.append(_start_bl_server(a))
    servers.append(_start_bl_server(b, a))
    servers.append(_start_bl_server(c, a, b))

    report = _wait_for_links({p.pid for p in servers}, 3)
    assert len(report.links) == 3
    (cycle,) = report.cycles
    assert sorted(cycle) == sorted(p.pid for p in servers)

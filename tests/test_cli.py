import argparse
import asyncio
import logging
import re

import pytest

from software_bus import (
    bl_client,
    bl_server,
    bus_check,
    ps_publish,
    ps_server,
    ps_subscribe,
    rr_reply,
    rr_request,
)
from software_bus._cli import (
    ConnectFailed,
    ConnectionLost,
    InvalidSubject,
    _keep_connected,
    build_server_arg_parser,
    check_subjects,
    connect,
    parse_bool,
    parse_timeout,
    print_received,
    reachable_from_other_machines,
    repeat,
    run_server,
    run_until_connection_lost,
    run_until_interrupted,
)
from software_bus.base_layer import DEFAULT_HOST, DEFAULT_PORT
from software_bus.client import BaseLayerClient
from software_bus.pubsub import CycleCheckRefused

from helpers import free_port

# Minimal valid argv for each script, so parse_args() succeeds.
_SCRIPT_ARGV = [
    (bl_server, []),
    (ps_server, []),
    (bl_client, []),
    (ps_subscribe, ["--subject", "a.b"]),
    (ps_publish, ["--subject", "a.b", "--message", "hi"]),
    (rr_reply, ["--accept-request", "svc", "--reply-message", "ok"]),
    (rr_request, ["--request-subject", "svc"]),
]


@pytest.mark.parametrize("value, expected", [("true", True), ("FALSE", False), ("True", True)])
def test_parse_bool_accepts_true_and_false_case_insensitively(value, expected):
    assert parse_bool(value) is expected


@pytest.mark.parametrize("value", ["yes", "1", ""])
def test_parse_bool_rejects_other_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_bool(value)


@pytest.mark.parametrize("value, expected", [("0", 0.0), ("1.5", 1.5), ("300", 300.0)])
def test_parse_timeout_accepts_numbers_of_seconds_from_zero(value, expected):
    assert parse_timeout(value) == expected


@pytest.mark.parametrize("value", ["-1", "abc", "", "nan"])
def test_parse_timeout_rejects_negative_or_non_numbers(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timeout(value)


@pytest.mark.parametrize("script, argv", [*_SCRIPT_ARGV, (bus_check, [])])
def test_every_script_accepts_debug_flag_defaulting_to_false(script, argv):
    parser = script.build_arg_parser()
    assert parser.parse_args(argv).debug is False
    assert parser.parse_args([*argv, "--debug", "true"]).debug is True


@pytest.mark.parametrize("script, argv", _SCRIPT_ARGV[2:])
def test_every_client_script_upstream_defaults_to_base_layer_default(script, argv):
    args = script.build_arg_parser().parse_args(argv)
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)


@pytest.mark.parametrize("server", [bl_server, ps_server])
def test_server_reconnect_option_defaults_to_false(server):
    assert server.build_arg_parser().parse_args([]).reconnect is False
    assert server.build_arg_parser().parse_args(["--reconnect", "true"]).reconnect is True


def test_build_server_arg_parser_names_the_server_in_help():
    help_text = build_server_arg_parser("Run a thing.", "thing_server").format_help()
    assert "Run a thing." in help_text
    assert "upstream thing_server" in help_text


def test_check_subjects_passes_subjects_the_check_accepts():
    checked = []
    check_subjects(checked.append, ["a.b", "c"])
    assert checked == ["a.b", "c"]


def test_check_subjects_raises_invalid_subject_for_the_first_rejected_one():
    def check(subject):
        if subject.startswith("bad"):
            raise ValueError("no good")

    with pytest.raises(InvalidSubject) as exc_info:
        check_subjects(check, ["ok", "bad1", "bad2"])

    assert "'bad1'" in str(exc_info.value) and "no good" in str(exc_info.value)
    assert isinstance(exc_info.value.error, ValueError)


@pytest.mark.asyncio
async def test_repeat_calls_action_count_times_with_interval_between():
    loop = asyncio.get_event_loop()
    call_times = []

    async def action():
        call_times.append(loop.time())

    await repeat(action, 3, 0.05)

    assert len(call_times) == 3
    gaps = [later - earlier for earlier, later in zip(call_times, call_times[1:])]
    assert all(gap >= 0.04 for gap in gaps)


@pytest.mark.asyncio
async def test_repeat_with_zero_count_never_calls_action():
    calls = []

    async def action():
        calls.append(1)

    await repeat(action, 0, 0.05)

    assert calls == []


def test_print_received_with_time_stamp(capsys):
    print_received("a.b: hi", time_stamp=True)
    out = capsys.readouterr().out
    assert re.fullmatch(r"\[\d{4}-\d{2}-\d{2}T[\d:.]+\] a\.b: hi\n", out)


def test_print_received_without_time_stamp(capsys):
    print_received("a.b: hi", time_stamp=False)
    assert capsys.readouterr().out == "a.b: hi\n"


def test_run_until_interrupted_runs_coroutine_to_completion():
    ran = []

    async def main():
        ran.append(True)

    run_until_interrupted(main())

    assert ran == [True]


def test_run_until_interrupted_swallows_keyboard_interrupt():
    async def main():
        raise KeyboardInterrupt

    run_until_interrupted(main())  # must not raise


class _FakeClient:
    """Stands in for BaseLayerClient/PubSubClient: just holds the error callback."""

    def __init__(self):
        self.on_error = None

    def register_connection_error_callback(self, callback):
        self.on_error = callback


@pytest.mark.asyncio
async def test_run_until_connection_lost_stops_body_and_raises_on_loss():
    client = _FakeClient()
    body_cancelled = []

    async def body():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            body_cancelled.append(True)
            raise

    task = asyncio.ensure_future(run_until_connection_lost(client, body()))
    await asyncio.sleep(0.01)
    error = ConnectionResetError("peer gone")
    client.on_error(object(), error)

    with pytest.raises(ConnectionLost) as exc_info:
        await asyncio.wait_for(task, timeout=1)
    assert exc_info.value.error is error
    assert body_cancelled == [True]


@pytest.mark.asyncio
async def test_run_until_connection_lost_returns_when_body_finishes():
    ran = []

    async def body():
        ran.append(True)

    await run_until_connection_lost(_FakeClient(), body())

    assert ran == [True]


@pytest.mark.asyncio
async def test_run_until_connection_lost_propagates_body_error_without_loss():
    async def body():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await run_until_connection_lost(_FakeClient(), body())


def test_run_until_interrupted_exits_with_status_1_on_connection_loss():
    async def main():
        raise ConnectionLost(ConnectionResetError("peer gone"))

    with pytest.raises(SystemExit) as exc_info:
        run_until_interrupted(main())

    # sys.exit(<message>) prints the message to stderr and exits with status 1
    assert "connection to server lost" in str(exc_info.value.code)


@pytest.mark.asyncio
async def test_connect_raises_connect_failed_when_server_unreachable():
    port = free_port()  # nothing listening here
    client = BaseLayerClient()

    with pytest.raises(ConnectFailed) as exc_info:
        await connect(client, "127.0.0.1", port)

    assert f"127.0.0.1:{port}" in str(exc_info.value)
    assert isinstance(exc_info.value.error, OSError)


def test_run_until_interrupted_exits_with_status_1_on_connect_failure():
    async def main():
        raise ConnectFailed("127.0.0.1", 1, ConnectionRefusedError("refused"))

    with pytest.raises(SystemExit) as exc_info:
        run_until_interrupted(main())

    assert "cannot connect to 127.0.0.1:1" in str(exc_info.value.code)


# --- reconnecting servers -------------------------------------------------------------


class _FakeConnection:
    def __init__(self, lost: bool) -> None:
        self.lost = lost

    async def wait_ended(self) -> None:
        if not self.lost:
            await asyncio.Event().wait()  # stays connected


class _FakeNode:
    """establish_connection plays `outcomes` in turn: an exception to raise, or a
    _FakeConnection to return."""

    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)
        self.attempts = 0

    async def establish_connection(self, host, port):
        self.attempts += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.mark.asyncio
async def test_reconnect_delay_doubles_up_to_the_maximum_and_restarts_after_a_connection():
    unreachable = ConnectionRefusedError("refused")
    node = _FakeNode(
        [unreachable] * 8
        + [_FakeConnection(lost=True)]
        + [unreachable, CycleCheckRefused("could not tell", retryable=True)]
        + [_FakeConnection(lost=False)]
    )
    delays = []

    async def sleep(delay):
        delays.append(delay)

    task = asyncio.ensure_future(_keep_connected(node, "127.0.0.1", 1, sleep))
    try:
        await asyncio.sleep(0.05)
        assert node.attempts == 12 and not task.done()  # the last connection is kept
    finally:
        task.cancel()
    expected = [1, 2, 4, 8, 16, 32, 60, 60] + [1, 2, 4]  # back to 1 s once connected
    assert len(delays) == len(expected)
    for delay, longest in zip(delays, expected):
        assert longest * 0.5 <= delay <= longest  # random factor between 0.5 and 1


@pytest.mark.asyncio
async def test_reconnect_ends_with_connect_failed_on_a_lasting_refusal_at_the_first_attempt():
    node = _FakeNode([CycleCheckRefused("it would create a cycle")])

    async def sleep(delay):
        pytest.fail("must not try again")

    with pytest.raises(ConnectFailed, match="create a cycle"):
        await _keep_connected(node, "127.0.0.1", 1, sleep)


@pytest.mark.asyncio
async def test_reconnect_stops_and_logs_on_a_lasting_refusal_after_the_first_attempt(caplog):
    node = _FakeNode([ConnectionRefusedError("refused"), CycleCheckRefused("it would create a cycle")])

    async def sleep(delay):
        pass

    with caplog.at_level(logging.WARNING, logger="software_bus._cli"):
        await asyncio.wait_for(_keep_connected(node, "127.0.0.1", 1, sleep), timeout=1)
    assert node.attempts == 2
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "create a cycle" in errors[0].getMessage()


# --- listening on an address other machines can reach ---------------------------------


@pytest.mark.parametrize(
    "host, reachable",
    [
        ("127.0.0.1", False),
        ("127.5.5.5", False),
        ("::1", False),
        ("localhost", False),
        ("0.0.0.0", True),
        ("192.168.1.10", True),
        ("bus-host.example", True),  # a host name isn't resolved: it counts as reachable
    ],
)
def test_reachable_from_other_machines(host, reachable):
    assert reachable_from_other_machines(host) is reachable


class _ListeningNode:
    """Pretends to listen, so the test opens no real socket on a public address."""

    async def accept_connection(self, host, port):
        pass

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_a_server_warns_only_about_listen_addresses_other_machines_can_reach(caplog):
    with caplog.at_level(logging.WARNING, logger="software_bus._cli"):
        task = asyncio.ensure_future(
            run_server(_ListeningNode(), [], [("0.0.0.0", 8787), ("127.0.0.1", 8788)])
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "0.0.0.0:8787" in warnings[0] and "no authentication" in warnings[0]

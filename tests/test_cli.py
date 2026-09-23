import argparse
import asyncio
import re

import pytest

from software_bus import bl_client, bl_server, ps_publish, ps_server, ps_subscribe
from software_bus._cli import (
    build_server_arg_parser,
    parse_bool,
    print_received,
    repeat,
    run_until_interrupted,
)
from software_bus.base_layer import DEFAULT_HOST, DEFAULT_PORT

# Minimal valid argv for each script, so parse_args() succeeds.
_SCRIPT_ARGV = [
    (bl_server, []),
    (ps_server, []),
    (bl_client, []),
    (ps_subscribe, ["--subject", "a.b"]),
    (ps_publish, ["--subject", "a.b", "--message", "hi"]),
]


@pytest.mark.parametrize("value, expected", [("true", True), ("FALSE", False), ("True", True)])
def test_parse_bool_accepts_true_and_false_case_insensitively(value, expected):
    assert parse_bool(value) is expected


@pytest.mark.parametrize("value", ["yes", "1", ""])
def test_parse_bool_rejects_other_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_bool(value)


@pytest.mark.parametrize("script, argv", _SCRIPT_ARGV)
def test_every_script_accepts_debug_flag_defaulting_to_false(script, argv):
    parser = script.build_arg_parser()
    assert parser.parse_args(argv).debug is False
    assert parser.parse_args([*argv, "--debug", "true"]).debug is True


@pytest.mark.parametrize("script, argv", _SCRIPT_ARGV[2:])
def test_every_client_script_upstream_defaults_to_base_layer_default(script, argv):
    args = script.build_arg_parser().parse_args(argv)
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)


def test_build_server_arg_parser_names_the_server_in_help():
    help_text = build_server_arg_parser("Run a thing.", "thing_server").format_help()
    assert "Run a thing." in help_text
    assert "upstream thing_server" in help_text


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

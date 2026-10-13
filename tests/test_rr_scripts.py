import asyncio
import time

import pytest

from software_bus import PubSubNode, ReplyStatus, RequestReplyClient, rr_reply, rr_request
from software_bus._cli import ConnectFailed, ConnectionLost, InvalidSubject, NoReply
from software_bus.base_layer import DEFAULT_HOST, DEFAULT_PORT

from helpers import free_port


async def start_node():
    node = PubSubNode()
    server = await node.accept_connection(port=0)
    return node, server.sockets[0].getsockname()[1]


async def stop(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_rr_reply_arg_parser_requires_subjects_and_reply_message():
    parser = rr_reply.build_arg_parser()
    for argv in ([], ["--accept-request", "svc"], ["--reply-message", "ok"]):
        with pytest.raises(SystemExit):
            parser.parse_args(argv)


def test_rr_reply_arg_parser_parses_comma_separated_subjects():
    args = rr_reply.build_arg_parser().parse_args(
        ["--accept-request", "a.b, c.d ,e", "--reply-message", "ok"]
    )
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)
    assert args.accept_request == ["a.b", "c.d", "e"]
    assert args.reply_message == "ok"
    assert args.time_stamp is True


def test_rr_request_arg_parser_requires_request_subject():
    with pytest.raises(SystemExit):
        rr_request.build_arg_parser().parse_args([])


def test_rr_request_arg_parser_defaults():
    args = rr_request.build_arg_parser().parse_args(["--request-subject", "svc"])
    assert args.upstream == (DEFAULT_HOST, DEFAULT_PORT)
    assert args.request_subject == "svc"
    assert args.request_message == ""
    assert args.timeout == 300
    assert args.continue_waiting is True
    assert args.time_stamp is True


@pytest.mark.parametrize("value", ["-1", "soon"])
def test_rr_request_arg_parser_rejects_bad_timeout(value):
    with pytest.raises(SystemExit):
        rr_request.build_arg_parser().parse_args(["--request-subject", "svc", "--timeout", value])


@pytest.mark.asyncio
async def test_rr_reply_run_prints_requests_and_replies(capsys):
    node, port = await start_node()
    task = asyncio.ensure_future(
        rr_reply.run("127.0.0.1", port, ["svc.a", "svc.b"], "ok", time_stamp=False)
    )
    requester = RequestReplyClient()
    replies = []
    try:
        await asyncio.sleep(0.1)  # let rr_reply connect and accept
        await requester.connect("127.0.0.1", port)
        for subject, message in (("svc.a", b"one"), ("svc.b", b"")):
            await requester.send_request(
                subject, message, lambda *args: replies.append(args[1:]), timeout=0
            )
        await asyncio.sleep(0.1)

        assert capsys.readouterr().out.splitlines() == ["svc.a: one", "svc.b: "]
        assert replies == [
            (b"Reply for svc.a: ok", ReplyStatus.REPLY),
            (b"Reply for svc.b: ok", ReplyStatus.REPLY),
        ]
    finally:
        await requester.close()
        await stop(task)
        await node.close()


@pytest.mark.asyncio
async def test_rr_reply_run_with_time_stamp_prefixes_requests(capsys):
    node, port = await start_node()
    task = asyncio.ensure_future(rr_reply.run("127.0.0.1", port, ["svc"], "ok"))
    requester = RequestReplyClient()
    try:
        await asyncio.sleep(0.1)
        await requester.connect("127.0.0.1", port)
        await requester.send_request("svc", b"hi", lambda *args: None, timeout=0)
        await asyncio.sleep(0.1)
        out = capsys.readouterr().out.strip()
        assert out.startswith("[") and out.endswith("] svc: hi")
    finally:
        await requester.close()
        await stop(task)
        await node.close()


@pytest.mark.asyncio
async def test_rr_request_run_stops_after_first_reply_without_continue_waiting(capsys):
    node, port = await start_node()
    replier = RequestReplyClient()
    try:
        await replier.connect("127.0.0.1", port)

        async def on_request(request_subject, reply_subject, payload):
            await replier.send_reply(reply_subject, b"first:" + payload)
            await replier.send_reply(reply_subject, b"second")

        await replier.accept_request("svc", on_request)
        await asyncio.sleep(0.05)

        started = time.monotonic()
        await rr_request.run(
            "127.0.0.1", port, "svc", "hi", timeout=5, continue_waiting=False, time_stamp=False
        )
        assert time.monotonic() - started < 1  # didn't wait for the timeout
        assert capsys.readouterr().out.splitlines() == ["svc: first:hi"]
    finally:
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_rr_request_run_with_continue_waiting_prints_every_reply_until_the_timeout(capsys):
    node, port = await start_node()
    replier = RequestReplyClient()
    try:
        await replier.connect("127.0.0.1", port)

        async def on_request(request_subject, reply_subject, payload):
            await replier.send_reply(reply_subject, b"1")
            await replier.send_reply(reply_subject, b"2")

        await replier.accept_request("svc", on_request)
        await asyncio.sleep(0.05)

        await rr_request.run("127.0.0.1", port, "svc", timeout=0.3, time_stamp=False)
        # replies arrived, so the timeout isn't a failure: run returns normally
        assert capsys.readouterr().out.splitlines() == ["svc: 1", "svc: 2", "svc: timed out"]
    finally:
        await replier.close()
        await node.close()


@pytest.mark.asyncio
async def test_rr_request_run_without_any_reply_prints_timed_out_and_raises_no_reply(capsys):
    node, port = await start_node()
    try:
        with pytest.raises(NoReply):
            await rr_request.run("127.0.0.1", port, "nobody", timeout=0.1, time_stamp=False)
        assert capsys.readouterr().out.splitlines() == ["nobody: timed out"]
    finally:
        await node.close()


@pytest.mark.asyncio
async def test_rr_request_and_rr_reply_work_together(capsys):
    node, port = await start_node()
    replier = asyncio.ensure_future(
        rr_reply.run("127.0.0.1", port, ["svc"], "pong", time_stamp=False)
    )
    try:
        await asyncio.sleep(0.1)
        await rr_request.run(
            "127.0.0.1", port, "svc", "ping", timeout=5, continue_waiting=False, time_stamp=False
        )
        assert capsys.readouterr().out.splitlines() == ["svc: ping", "svc: Reply for svc: pong"]
    finally:
        await stop(replier)
        await node.close()


@pytest.mark.asyncio
async def test_rr_request_run_reports_a_lost_connection_not_a_timeout(capsys):
    node, port = await start_node()
    task = asyncio.ensure_future(
        rr_request.run("127.0.0.1", port, "svc", timeout=0, time_stamp=False)
    )
    try:
        await asyncio.sleep(0.1)  # let rr_request connect and send
        await node.close()
        with pytest.raises(ConnectionLost):
            await asyncio.wait_for(task, timeout=1)
        assert "timed out" not in capsys.readouterr().out
    finally:
        task.cancel()
        await node.close()


@pytest.mark.asyncio
async def test_rr_reply_run_raises_connection_lost_when_server_goes_away():
    node, port = await start_node()
    task = asyncio.ensure_future(rr_reply.run("127.0.0.1", port, ["svc"], "ok"))
    try:
        await asyncio.sleep(0.1)
        await node.close()
        with pytest.raises(ConnectionLost):
            await asyncio.wait_for(task, timeout=1)
    finally:
        task.cancel()
        await node.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "run",
    [
        lambda port: rr_reply.run("127.0.0.1", port, ["svc"], "ok"),
        lambda port: rr_request.run("127.0.0.1", port, "svc", timeout=1),
    ],
    ids=["rr_reply", "rr_request"],
)
async def test_run_raises_connect_failed_when_server_unreachable(run):
    with pytest.raises(ConnectFailed):
        await asyncio.wait_for(run(free_port()), timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "run",
    [
        lambda port: rr_reply.run("127.0.0.1", port, ["svc", "a.*"], "ok"),
        lambda port: rr_reply.run("127.0.0.1", port, ["x" * 65527], "ok"),
        lambda port: rr_request.run("127.0.0.1", port, "*"),
        lambda port: rr_request.run("127.0.0.1", port, "x" * 65496),
    ],
    ids=["rr_reply wild card", "rr_reply too long", "rr_request wild card", "rr_request too long"],
)
async def test_run_raises_invalid_subject_before_connecting(run):
    # no server on this port: the subject is rejected first
    with pytest.raises(InvalidSubject):
        await asyncio.wait_for(run(free_port()), timeout=5)

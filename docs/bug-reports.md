# Bug Reports

## Reported Bugs

- Test Hanged: tests/test_client.py::test_client_failing_connection_error_callback_stays_out_of_the_receive_loop hanged while running on Python 3.12.4.
    - Reported on: 15 October 2026
    - Solved on: 16 October 2026
    - Cause: a race in `BaseLayerNode.close()`. `client.connect()` can return before the node has handled the connection it accepted. A `close()` in that gap doesn't see the connection, and the node then lists it after closing, so nothing closes it, and `Server.wait_closed()` (Python 3.12.1+) waits for it forever. The test's failing callback was not involved.
    - Fix: `close()` marks the node closed first, and a connection that arrives after that is closed at once instead of listed. The same applies to `establish_connection`, which then raises a connection error. Covered by `test_close_closes_a_connection_accepted_but_not_handled_yet` and `test_establish_connection_closes_and_raises_if_the_node_closed_meanwhile` in `tests/test_base_layer.py`.
    - Verified on the affected machine: yes, on 21 October 2026 (macOS, Python 3.12.4); see the results below. The hang never happened on its own on Windows with Python 3.13; it was only shown by forcing the race. To verify, on the machine that hung (Python 3.12.4), on the branch with the fix (if the package isn't installed, prefix each command with `PYTHONPATH=src`, or in PowerShell set `$env:PYTHONPATH = "src"` first):
        1. Check the branch and the Python version: `git checkout fix-close-before-accept`, then `python --version`.
        2. Run the test that hung 100 times. Every run must pass and none may hang: one passing run proves little, because the race is rare.
            - bash: `for i in $(seq 100); do timeout 60 python -m pytest -q tests/test_client.py::test_client_failing_connection_error_callback_stays_out_of_the_receive_loop || { echo "FAILED on run $i"; break; }; done`
            - PowerShell (no timeout, so stop a hung run with Ctrl-C): `for ($i = 1; $i -le 100; $i++) { python -m pytest -q tests/test_client.py::test_client_failing_connection_error_callback_stays_out_of_the_receive_loop; if ($LASTEXITCODE) { "FAILED on run $i"; break } }`
        3. Run the two new tests, which force the race, so they don't depend on timing: `python -m pytest -v tests/test_base_layer.py -k "not_handled_yet or closed_meanwhile"`.
        4. Run the full test suite once: `python -m pytest -q`.
        5. Optional: run step 2 on `main` first. If it hangs on `main` but not on the branch, that confirms the machine hits the race and that the fix stops it. If it never hangs on `main` either, step 2 shows little, and the result rests on steps 3 and 4.
    - Verification Result:
        - Ran on MacOS with Python 3.12.4.
        - Test 2: Does not have timeout command. Left out the ```timeout 60```. Test failed on run 1 or run 3.
        - Test 3: Passed.
        - Test 4: Passed.
        - Test 5: Test Hanged.
    - Follow-up (20 October 2026): the hang is gone on the branch, but test 2 still failed now and then with a `TimeoutError`: the client didn't notice the node had closed. `tmp/close_race_probe.py` on the Mac showed that in about 1 in 200 runs the client never noticed, even after 10 s. That matches an asyncio bug that was forced on Windows: asyncio leaves the socket open when the node closes after the socket is accepted but before its transport exists. The test now waits until the node lists the connection before closing it. The asyncio bug is recorded in `docs/known-issues.md` ("A connection arriving as a node closes can stay open"). Test 2 needs re-running on the Mac.
        - Result:
            - Test 2 completed without error on Mac.

- Flaky Test: tests/test_request_reply.py::test_failing_reply_callback_keeps_the_wait failed once in a full test suite run on WSL (Ubuntu 20.04, Python 3.8.10). It passed in 5 reruns on its own, and with the rest of `test_request_reply.py`. The failure message wasn't kept.
    - Reported on: 16 October 2026
    - Suspected cause: not confirmed. The test relied on fixed sleeps (0.05 s for the acceptor's subscription to reach the node, 0.2 s for both replies to arrive), which may be too short when the machine is busy. The run that failed took 50 s, against about 35 s for a normal run, so the machine may well have been busy.
    - Investigation (16 October 2026): the failure didn't happen again in 6 more full runs on WSL, or in 40 runs of the test alone with every CPU busy. The code it tests has no race: the requester's subscribe reaches the node before its request on the same connection, and the reply that ends the wait removes the request at once. That leaves the two sleeps as the only timing the test depends on.
    - Fix: the test now waits for each condition instead of a fixed time (`wait_until`): for the node to have the acceptor's subscription, then for both replies. A real fault still fails it, after at most `wait_until`'s 2 s.
    - Solved on: 16 October 2026, if the sleeps were the cause. Since the failure couldn't be reproduced, this is not proven; if it fails again, keep the output.
    - Done (22 October 2026): switched the other fixed sleeps in `tests/test_request_reply.py` (about 20) to `wait_until` the same way, since they could fail the same way on a busy machine. The sleeps left check that something does *not* happen (e.g. "past the timeout: no TIMED_OUT call may follow"), or delay a callback on purpose: there is no condition to wait for there.

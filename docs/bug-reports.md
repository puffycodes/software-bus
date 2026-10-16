# Bug Reports

## Reported Bugs

- Test Hanged: tests/test_client.py::test_client_failing_connection_error_callback_stays_out_of_the_receive_loop hanged while running on Python 3.12.4.
    - Reported on: 15 October 2026
    - Solved on: 16 October 2026
    - Cause: a race in `BaseLayerNode.close()`. `client.connect()` can return before the node has handled the connection it accepted. A `close()` in that gap doesn't see the connection, and the node then lists it after closing, so nothing closes it, and `Server.wait_closed()` (Python 3.12.1+) waits for it forever. The test's failing callback was not involved.
    - Fix: `close()` marks the node closed first, and a connection that arrives after that is closed at once instead of listed. The same applies to `establish_connection`, which then raises a connection error. Covered by `test_close_closes_a_connection_accepted_but_not_handled_yet` and `test_establish_connection_closes_and_raises_if_the_node_closed_meanwhile` in `tests/test_base_layer.py`.

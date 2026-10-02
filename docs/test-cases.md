# Test Cases

All 276 automated test cases in `tests/`, grouped by test file and written in plain English. A test that runs once per input case is listed once, with the inputs it covers and a count.

Some behaviour can't be tested automatically; those tests are described at the end, under [Manual tests](#manual-tests), and are not included in the counts.

## Subject matching (`test_subject_matcher.py`, 26 cases)
- **Exact matcher (4 cases):** a subject matches only an identical subject. `a.b` matches `a.b` but not `a.c` or `a.b.c`, and an empty subject matches an empty subject.
- **Wildcard pattern matcher (22 cases):**
  - `a.b` matches `a.b`, `a.*`, `*.b` and `*.*`.
  - `a.b.c` matches `a.*.c`.
  - Different words don't match: `a.b` vs `a.c`.
  - A different number of parts doesn't match: `a.b` vs `a.b.c`, and `a.b.c` vs `a.*`.
  - `*` matches `*`. An empty subject matches `*`, and an empty subject matches an empty subject.
  - A `*` in the *published* subject is just a character, not a wildcard: `a.*` doesn't match a subscription to `a.b`, but does match a subscription to `a.*`.
  - Matching is case-sensitive: `A.b` doesn't match `a.b`, and `a.b` doesn't match `A.*`.
  - Only a whole `*` part is a wildcard: `ab` doesn't match `a*`, which only matches `a*` itself.
  - Empty parts count like any other part: `a..b` matches `a.*.b` and `a..b`, `a.b` doesn't match `a..b`, `a.` matches `a.*`, and `a` doesn't match `a.*`.

## Base layer node (`test_base_layer.py`, 27 cases)
- **Setup and listening**
  - A new node isn't listening and has no connections.
  - Listening with no arguments uses the default address, 127.0.0.1:8787.
  - A node can listen on several addresses at once.
  - Asking to listen on an address it already listens on does nothing.
  - Using the node in an `async with` block starts it listening, and closes it at the end.
- **Connecting**
  - Two nodes can connect, and each records the connection on the correct side (upstream or downstream).
  - The "new connection" callbacks are called for both new upstream and new downstream connections.
- **Framing**
  - Each message is sent as a 4-byte big-endian length followed by the payload, and an empty payload is just the 4-byte length.
  - Message boundaries are kept however TCP delivers the bytes: several messages (one of them empty) in a single read, and one message split in the middle of its length and of its payload.
  - A 2 MiB message is relayed intact.
- **Relaying data**
  - Data from a downstream connection goes to every upstream connection and every other downstream connection.
  - Data from an upstream connection goes to every downstream connection and every other upstream connection.
  - Data is never sent back to the connection it came from.
  - Registering your own upstream receive callback replaces the default relaying.
  - Registering your own downstream receive callback replaces the default relaying.
  - A receive callback can be an async function.
  - If a receive callback fails, only that one message is lost: the connection stays open, later messages still arrive, no connection error is reported, and the failure is logged.
- **Connection errors**
  - When an upstream peer drops, the connection is removed and the upstream error callback is called.
  - When a downstream peer drops, the connection is removed and the downstream error callback is called.
  - An error callback can be an async function, for upstream and for downstream connections.
  - The default error callbacks log a message.
  - If sending to one connection fails while relaying, only that connection is removed; the others keep receiving.
- **Closing**
  - Closing a node empties its lists of connections and listening addresses.
  - Closing a node doesn't report its own connections as errors, but the peer at the other end does see its connection dropped.
  - Closing a node twice, or closing one that never listened, does no harm.

## Base layer client (`test_client.py`, 15 cases)
- **Connecting**
  - A new client isn't connected and has no receive callback.
  - The client's default address to connect to is the same as the node's default listening address.
  - After connecting, the client holds the connection.
  - Sending without connecting first raises an error.
- **Sending and receiving**
  - Data sent by a client reaches the node.
  - The receive callback is called with each piece of data received.
  - The receive callback can be an async function.
  - Receiving data with no callback registered causes no error.
  - If the receive callback fails, only that one message is lost: the client stays connected, the next message still arrives, and the failure is logged.
- **Errors and closing**
  - A socket error while receiving ends the receive loop quietly instead of crashing it.
  - When the node drops the connection, the connection error callback is called.
  - The default connection error callback logs a message.
  - Closing the client does not call the connection error callback.
  - Closing a client that never connected, or closing it twice, does no harm.
  - A failed send is reported to the connection error callback and also raised to the caller.

## Publish/subscribe (`test_pubsub.py`, 53 cases)
- **Wire format (21 cases)**
  - Subscription and publish messages have exactly the byte layout in `data-format.md`, including a subject's length counted in UTF-8 bytes.
  - Bytes after a subscription's subject are ignored.
  - A subscription message survives encoding and decoding unchanged.
  - A publish message with binary content survives encoding and decoding unchanged.
  - Edge cases survive encoding and decoding: an empty subject, a subject with non-ASCII characters, and an empty payload (4 cases).
  - A message with an unknown type tag is rejected.
  - Malformed messages are rejected (7 cases): empty data, a subscription without its subscribe/unsubscribe byte, that byte with an invalid value, a cut-off subject length, a subscription or publish shorter than its stated subject length, and a subject that isn't valid UTF-8.
  - A subject exactly 65535 bytes long is accepted.
  - Subjects over 65535 bytes are rejected (3 cases), including one that's only too long once encoded as UTF-8.
- **Client subscribing and unsubscribing**
  - Subscribing with a subject that's too long raises an error and leaves nothing registered.
  - Only the first callback on a subject sends a subscription to the node.
  - Only removing the last callback on a subject sends an unsubscription.
  - Unsubscribing from a subject the client never subscribed to, or with a callback it never registered, sends nothing and keeps the existing callbacks.
  - Every callback on the same client and subject is called for a publish.
  - A subscription callback can be an async function.
  - If one subscription callback fails, the others still get the publish. This includes the client's own publishes, and the failure isn't raised to the publisher.
- **Delivery**
  - A publish reaches only clients subscribed to its subject.
  - A publish reaches a wildcard subscription that matches it.
  - A publish reaches every subscribed client on the same node.
  - A publish to an unrelated subject isn't delivered.
  - A connection subscribed under two matching subjects (e.g. `a.b` and `a.*`) gets only one copy.
  - Different connections with overlapping subscriptions each get their copy.
  - A client receives its own publish exactly once.
  - A node never sends a publish back to the connection it came from.
  - A publish doesn't bounce back and forth forever between two nodes that both have subscribers.
- **Propagation through a tree of nodes**
  - A subscription travels up the tree, and publishes then flow down to the subscriber.
  - The same works for a wildcard subscription.
  - A subscription arriving from upstream is passed downstream and to other upstream connections.
  - A node that joins late is told about the subscriptions that already exist.
  - A subscription passed on to a sibling client is just logged by that client.
- **Unsubscribing**
  - Unsubscribing stops delivery and is passed upstream.
  - An unsubscribe isn't passed on while other subscribers to the subject remain.
  - Unsubscribing from one of two overlapping subjects keeps delivery through the other.
  - An unsubscribe for a subject the connection never subscribed to isn't passed on (otherwise nodes could bounce unsubscribes back and forth).
  - When every subscriber leaves, the subscriptions between nodes are torn down.
  - A downstream connection dropping counts as unsubscribing from all its subjects, and this is passed on.
  - The same applies when an upstream connection drops.
  - Closing a node sends no unsubscribes (its connections just end) and forgets every subscription.
- **Robustness**
  - A node logs and ignores a malformed message, and keeps the connection.
  - A client logs and ignores a malformed message, and keeps receiving.
  - A client logs and ignores reachability queries and replies (not as malformed), replies nothing, and keeps receiving.
  - A client's connection error callback is called when its node goes away.

## Cycle prevention (`test_cycle_prevention.py`, 43 cases)
- **Wire format (13 cases)**
  - Hello, query and reply messages (all three results) survive encoding and decoding unchanged (5 cases).
  - Their byte layout is exactly as specified.
  - Malformed ones are rejected (5 cases): a Hello one byte too short or one byte too long, a query that's too short, a reply with no result byte, and a result value outside 0–2.
  - Node and query IDs of the wrong length can't be encoded.
  - Every node gets its own random 16-byte ID.
- **Accepting and refusing connections**
  - A connection between two nodes is accepted.
  - A connection from a node to itself is refused.
  - A second connection to a node it's already connected to is refused.
  - A connection that would close a triangle is refused.
  - A connection that would close a longer cycle is refused.
  - A connection to a base layer node is refused, because it never identifies itself.
  - A connection to any peer that never sends Hello is refused.
  - A check that keeps getting "unknown" retries, then gives up and refuses.
  - A "found" reply refuses the connection at once, without waiting for the other nodes to reply.
  - A node that fails before replying counts as "not found", so the check doesn't wait for it.
  - `ps_server` exits with an error if an upstream connection is refused.
- **The pending period**
  - Messages received on a connection still being checked are held until it's accepted.
  - Nothing is sent on a connection still being checked until it's accepted.
  - Once accepted, subscriptions flow both ways.
  - Closing a node while a check is still waiting for Hello ends the check straight away with an error.
  - The peer dropping during the check ends it straight away with an error.
  - Messages held from a connection that is then refused are thrown away: its subscriptions aren't recorded and its publishes aren't delivered.
  - If the connection fails while its held messages are being processed, the rest are dropped, so none of them tags a dead connection.
- **Answering queries**
  - A query for the node itself is answered "found".
  - A query with no other nodes to pass it to is answered "not found".
  - A query is passed only to other nodes (not clients), and their answer is passed back.
  - A passed-on query that times out is answered "unknown".
  - The same query arriving twice is answered "unknown".
  - A node that's running a check of its own answers "unknown".
  - A query arriving on a connection still being checked is answered "not found", even when it asks for this node.
  - A query arriving on a connection still being checked while the node is running a check of its own is answered "unknown".
  - A reply that arrives after the query has been answered is ignored.
  - A client quietly ignores Hello messages.
- **Links added at the same time**
  - A node that adds two links at once checks them one at a time.
  - Two nodes adding links at the same time can't close a cycle between them.

## Shared script helpers (`test_cli.py`, 28 cases)
- **Options and parsing**
  - `true`/`false` options accept any capitalisation (3 cases).
  - Other values like `yes`, `1` or an empty string are rejected (3 cases).
  - Every script accepts `--debug`, and it defaults to false (6 scripts).
  - Every client script's `--upstream` defaults to 127.0.0.1:8787 (3 scripts).
  - The server help text names the right server.
- **Runtime helpers**
  - Connecting to an unreachable server raises a clear "cannot connect" error.
  - Repeat calls the action the given number of times, with the interval in between.
  - Repeat with a count of zero never calls the action.
  - Received messages are printed with a time stamp.
  - Received messages are printed without a time stamp.
  - "Run until connection lost" returns normally when the script's work finishes.
  - It stops the work and raises "connection lost" when the connection drops.
  - It passes on the work's own error when the connection wasn't lost.
- **Exiting**
  - A script's coroutine is run to completion.
  - Ctrl-C exits quietly.
  - A connection failure exits with status 1.
  - A lost connection exits with status 1.

## Base layer scripts (`test_scripts.py`, 21 cases)
- **Address parsing**
  - An `ip:port` address is parsed correctly.
  - Malformed addresses are rejected (4 cases): no port, no host, an empty port, and a non-numeric port.
- **`bl_server`**
  - With no options it has no upstream and no listen addresses.
  - Repeated `--upstream` and `--listen` options add up.
  - `--debug` is parsed.
  - Running it listens and connects upstream.
  - An unreachable upstream raises a clear "cannot connect" error.
  - An address already in use raises a clear "cannot listen" error.
  - Losing its upstream connection later doesn't stop it: it keeps relaying between its other connections.
- **`bl_client`**
  - Its default address matches the base layer default, and `--time-stamp` defaults to true.
  - `--message` defaults to none.
  - Custom repeat options are parsed.
  - It sends its message and prints what it receives.
  - It sends the message the requested number of times.
  - With no message, it sends nothing but still listens.
  - An unreachable server raises "cannot connect".
  - The server going away raises "connection lost".
  - It stops repeating when the server goes away.

## Pub/sub scripts (`test_ps_scripts.py`, 20 cases)
- **Subject list parsing**
  - A comma-separated list is split and its whitespace trimmed.
  - An empty list is rejected.
- **`ps_server`**
  - With no options it has no addresses.
  - Repeated options add up.
  - Running it listens and passes subscriptions upstream.
  - An unreachable upstream raises "cannot connect".
  - An address already in use raises "cannot listen".
  - Losing its upstream connection later doesn't stop it: its clients can still publish and subscribe.
- **`ps_subscribe`**
  - `--subject` is required.
  - Comma-separated subjects are parsed.
  - It prints each publish it receives.
  - Printing without a time stamp omits it.
  - With no subjects, it subscribes to nothing.
  - An unreachable server raises "cannot connect".
  - The server going away raises "connection lost".
- **`ps_publish`**
  - `--subject` and `--message` are both required.
  - Custom options are parsed.
  - It publishes the requested number of times.
  - An unreachable server raises "cannot connect".
  - The server going away partway through the repeats raises "connection lost".

## `bus_check` (`test_bus_check.py`, 43 cases)
- **Recognising node processes (11 cases)**
  - Nodes are recognised when started as `python -m software_bus.bl_server` or `ps_server`, including under a full Python path on Linux or Windows.
  - The installed `bl_server.exe` is recognised.
  - A script run by path is recognised, including with `python3 -u`.
  - A client, pytest, `grep bl_server`, a `python -c` that only mentions the name, and an empty command line are not treated as nodes.
- **Windows launchers**
  - Only the Python child of a Windows launcher counts as the node.
  - A node started by a node of a different kind is kept.
  - Without parent information, every node is kept.
- **Finding cycles**
  - A tree has no cycles.
  - A triangle is found.
  - Two links between the same pair of nodes form a cycle.
  - A node connected to itself forms a cycle.
  - Each cycle is reported as its path, in order.
  - Separate cycles are each found.
- **Finding links**
  - The two ends of one connection make a single link.
  - Connections to clients are ignored.
  - A process's own loopback connection off its listening port is ignored (the asyncio case on Windows).
  - A node really connected to its own listening port is kept.
  - A node listening on a wildcard address (0.0.0.0) is matched.
  - Connections to other machines, and node connections whose other end is hidden, are counted for the warnings.
  - Hidden connections that no node takes part in are ignored.
- **Report and exit status**
  - Listening addresses are filled in and the cycle is found.
  - A node whose sockets the operating system hides gets a warning.
  - A node that simply has no sockets (started with neither `--listen` nor `--upstream`) gets no warning.
  - Whether a node's sockets are hidden is decided by asking the operating system about that process: a refusal means hidden, while no sockets or a `--pid` that isn't running does not.
  - Collecting finds the nodes and drops a Windows launcher. A `--pid` process is added with kind `node`, but a `--pid` that is already a known node keeps its own kind. Only listening and established sockets are kept.
  - Without `psutil` installed, the check can't run and says how to install it.
  - If the operating system refuses to list connections, the check can't run.
  - With everything visible, no warnings are printed.
  - The report lists the nodes and the cycle.
  - With no cycles, the report says "No cycles found."
  - The exit status is 1 with a cycle and 0 without one.
  - A check that can't run prints an error and returns exit status 2.
  - The script exits with the status the check returns.
  - Repeated `--pid` options add up.
- **With real `bl_server` processes**
  - Three servers in a tree have no cycle.
  - Three servers in a triangle have one cycle.

## Manual tests

These need something the automated tests can't set up, so they are run by hand. Record the date and result of the latest run with each one.

### `bus_check` warns about a node connection to a hidden process

Checks that a node connection whose other end belongs to a process the operating system hides is counted and reported as a warning. The automated tests cover this only with fake connections: a real one needs a process run by another user, which needs `sudo`.

- **Needs:** Linux (e.g. WSL), run as a normal user who can use `sudo`, with `psutil` installed for that user.
- **Steps**, each in its own terminal, from the repository root:
  1. As yourself, start a node: `python3 -m software_bus.ps_server --listen 127.0.0.1:8787`
  2. As root, connect a client to it: `sudo PYTHONPATH=src python3 -m software_bus.ps_subscribe --subject test`. (`PYTHONPATH=src` because root doesn't see your own install of the package.)
  3. As yourself, run the check: `python3 -m software_bus.bus_check`
- **Expected:**
  - The `ps_server` is listed as the only node, listening on 127.0.0.1:8787.
  - The report includes `warning: 1 node connection(s) go to a process the operating system hides; the check may be incomplete (try again as administrator/root)`. The node's own end of the connection is visible, but the subscriber's end belongs to root, so it is hidden.
  - `No cycles found.`, and exit status 0.
- **Clean up:** stop the subscriber and the server with Ctrl-C.
- **Last run:** 2026-09-30, passed.

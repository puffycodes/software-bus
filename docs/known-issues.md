# Known Issues

Limitations we know about and have decided, for now, not to fix. Each entry says what the issue is, what it affects, and why it is left as it is. If an issue is fixed later, remove its entry in the same change.

## No limit on connections or subscriptions per node

- **Decided:** 2026-10-15.
- **Issue:** A node accepts any number of connections, and keeps any number of subscriptions. Only the size of a message, a connection's send queue, and what a node holds during a cycle check are limited.
- **Effect:** A buggy client can use up a node's resources. Examples:
    - A client that reconnects in a loop without closing its old connections piles up connections on its node.
    - A client that subscribes to a new subject for each message, and never unsubscribes, adds a subject to every node in the tree, because subscriptions are passed on through the tree.
- **Why it is left:**
    - The bus is meant to run on one machine or a trusted private network ("Trust Model" in `design/base-layer.md`), and neither bug has come up.
    - A sensible subscription limit is hard to choose: a connection from another node carries the subscriptions of every node and client behind it, so a limit that suits a client connection could cut a healthy node connection.
- **If it is ever done:** neither limit needs a wire format change. A node over its connection limit can close a new connection straight away. A connection that goes over a subscription limit can be treated as failed, through the usual connection error path (as for the held messages limit), which unsubscribes it from everything. The subscription limit would likely need separate values for node and client connections; a node tells them apart by whether the peer sent Hello.

## A connection arriving as a node closes can stay open

- **Decided:** 2026-10-20.
- **Issue:** asyncio accepts an incoming socket in one step and creates its transport in a later task. If the node's `close()` closes the listening server in between, creating the transport fails (`Server._attach` asserts that the server is still open). asyncio only reports this in debug mode, and never closes the socket. The node never sees the connection, so it can't close it either. Seen with Python 3.12.4 on macOS, about once in 200 runs of a client connecting just before its node closes. On Windows with Python 3.13 it only happened when forced.
- **Effect:** The client's connection stays open, with nothing at the other end, until the garbage collector frees the leaked socket or the node's process exits. So the client doesn't notice that the node has gone. Nothing hangs: the node's `close()` still returns, because asyncio never registered the connection with the server.
- **Why it is left:**
    - The bug is in asyncio, and the node can't see the socket. A workaround would need asyncio internals, such as stopping accepts on the listening socket before closing it.
    - Nodes are normally closed by their process exiting, which closes the socket, so the client does notice then. Only a node closed inside a process that keeps running is affected, such as in tests.
- **If it is ever done:** check whether a later Python version fixes it in asyncio first. Tests that close a node and then wait for a client to notice must first wait until the node lists the connection, as `test_client_failing_connection_error_callback_stays_out_of_the_receive_loop` does.

## A repeated subscribe is passed on through the whole tree

- **Decided:** 2026-10-15.
- **Issue:** A node that receives a subscribe from a connection already tagged to that subject keeps the tag single, but still passes the subscribe on to every other connection ("Message Processing" under the node class in `design/publish-subscribe.md`, which calls this redundant but harmless). Each node it reaches does the same.
- **Effect:** Every new subscriber to a subject the tree already knows about sends a subscribe through the whole tree, where only the first subscriber needed to. Examples: many `ps_subscribe` processes on the same subject, or several request/reply acceptors on the same `_request.<subject>`. Only subscribe traffic grows; publishes are not affected.
- **Why it is left:**
    - The cost is extra subscribe messages when subscribers come and go, which is small for how the bus is used.
    - The current rule is simple and plainly correct. A smarter rule that is wrong would leave a node without a route to a subscriber, so publishes would silently not arrive: worse than the extra traffic.
- **If it is ever done:** in two steps, each changing the spec rule and `PubSubNode._handle_subscription` in `pubsub.py`, with tests for each case:
    - Simple: don't pass on a subscribe from a connection that was already tagged to the subject, as unsubscribes already act only when a tag was actually removed.
    - Full: decide per neighbour, mirroring the unsubscribe rule: tell neighbour N about a subject only if, before this subscribe, no connection other than N subscribed to it at this node. Take care that when N was the only subscriber and another connection now subscribes, N must be told.

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

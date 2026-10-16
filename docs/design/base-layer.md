# Base Layer Design

## Definitions

- **Maximum payload length** The largest payload a single message can carry, 16 MiB (16777216 bytes). See "Base Layer Wire Format" in `data-format.md`.
- **Send timeout** 30 seconds. The longest time data may wait to be handed to the operating system for one connection.
- **Close timeout** 5 seconds. The longest time a client's Close waits for the node to take data already sent.
- **Send queue limit** 64 MiB (four times the maximum payload length). The most data a node keeps waiting to be sent on one connection.
- **Reconnect delay** How long a server waits before trying an upstream connection again. The first delay is 1 second, and each failed attempt in a row doubles it, up to 60 seconds. Each delay is multiplied by a random factor between 0.5 and 1, so that servers that lost the same upstream node do not all try again at the same moment. The delay goes back to 1 second once a connection succeeds.

## Trust Model

The bus assumes that every process that connects to it is trusted and cooperative: it follows the specs in `docs/design/`, and does not try to read what it should not, impersonate another node or client, or harm the bus.

- The bus has no authentication, encryption or access control. Any process that can open a TCP connection to a node can take part in the bus, as a node or as a client, and everything it sends and receives travels in the clear.
- It is therefore meant to run on a single machine (the default address, 127.0.0.1) or on a private network where every machine and user is trusted. Listening on an address other machines can reach makes the bus available to all of them (see the bl_server Script).
- The rules in these specs protect against mistakes and failures — malformed messages, lost connections, peers that are too slow, messages over the maximum payload length, cycles — not against a peer that misbehaves on purpose.
- Known non-goals. A peer that does not follow the specs can, among other things:
    - send a Hello message with another node's Node ID, or answer Reachability Queries falsely, so that the cycle check refuses good connections or accepts one that closes a cycle (see Cycle Prevention in `publish-subscribe.md`);
    - subscribe to any subject, including with wild cards, and so receive every publish, including the requests and replies of other clients (see Reserved Subjects in `request-reply.md`);
    - publish to any subject, including another client's reply subject;
    - use up a node's resources, e.g. by opening many connections or making many subscriptions. The limits in these specs bound the size of a message and of a connection's send queue, but not the number of connections or subscriptions.
        - This applies to mistakes as well: a client that keeps opening connections without closing them, or keeps subscribing to new subjects without unsubscribing, is not stopped either. Limits were left out on purpose: the bus runs on one machine or a trusted network, and a subscription limit that suits a client would cut a node connection, which carries the subscriptions of every node and client behind it. See `docs/known-issues.md`.
- Protecting against such peers (e.g. authenticating peers, encrypting connections, or limiting who may subscribe or publish to a subject) would need changes to the wire format (`data-format.md`), and is not part of this design.

## Callbacks

These rules apply to every callback given to the Base Layer Node Class or the Base Layer Client Class: receive callbacks, connection error callbacks and new connection callbacks.

- A callback can be either synchronous or asynchronous.
- If a callback fails (raises an error), log the error with its details, and then carry on exactly as if the callback had returned normally. A failing callback is not a connection error, does not close or drop a connection, and is not reported to the caller of the operation that triggered it.
    - This is the same rule for every callback: a failure in code given to the bus costs only what that call was doing, never a connection.
    - In particular, a failing callback never changes what an operation reports: Establish Connection still returns the new connection, and Send still reports its own failure, not the callback's.
- A layer on top that registers its own callbacks (e.g. the Publish and Subscribe Node Class) is responsible for them; these rules are about the callbacks a user gives.

## Base Layer Node Class

Functionalities

- **Instantiation** Create an instance of the class.

- **Accept Connection** Start accepting TCP connections from downstream instances on a given IP address (default 127.0.0.1) and a given port (default 8787).
    - The instance can accept connections on multiple IP/port pairs.
    - The instance will not be accepting connections on any IP/port when it is instantiated.
    - If the instance is already accepting connections on the given IP/port, this will do nothing.
    - If it cannot accept connections on the given IP/port (e.g. the address is already in use, or not one of this machine's), report the error to the caller. The IP/port is not added to the list, and nothing else changes.
- **Establish Connection** Create a TCP connection to an upstream instance.
    - If the connection cannot be made (e.g. nothing accepts connections at that address), report the error to the caller. Nothing is added to the list of upstream connections, and no callback is called.

- **Register Upstream Receive Callback** Provide a function to call when data are received from any of the upstream connections.
    - A default upstream receive callback will be registered during instantiation.
        - The default upstream receive callback will re-send the data received from a upstream connection to all the downstream connections, as well as all the upstream connections, other than itself.
- **Register Downstream Receive Callback** Provide a function to call when data are received from any of the downstream connections.
    - A default downstream receive callback will be registered during instantiation.
        - The default downstream receive callback will re-send the data received from a downstream connection to all the upstream connections, as well as all the downstream connections, other than itself.
- **Action Upon Receiving of Data** Action depends on the connection that the data is received from.
    - If the data is received from an upstream connections, call the upstream receive callback function with the data as a parameter.
    - If the data is received from a downstream connections, call the downstream receive callback function with the data as a parameter.
    - If the callback fails, follow Callbacks: log it and carry on with the next data. The connection is kept, and this is not a connection error.
- **Sending Data** Whenever the node sends data to a connection (relaying, or on behalf of a layer on top):
    - If the data is longer than the maximum payload length, send nothing to any connection, and log an error. This is not a connection error: the connections are kept.
        - A node relays only data it has received, which is never longer than the limit, so this only happens when a layer on top sends too much.
    - Each connection has its own send queue, sent in order by a task of its own. Sending data to a connection adds it to that connection's queue, and does not wait for it to be sent.
        - This keeps one slow connection from holding up the others. Without it, relaying data to a peer that has stopped reading would block the connection the data came from, and so every message from it to every other peer, for as long as that peer stays silent.
        - Messages to one connection are always sent in the order they were queued. There is no ordering between different connections.
    - A connection is too slow if:
        - adding the data would make its queue hold more than the send queue limit, or
        - the send timeout passes while the data at the front of its queue is still being sent.
    - A connection that is too slow has failed (see Exceptions Related to a Connection). The data waiting in its queue is discarded.
        - The rest of the tree loses that branch, as for any other failed connection. That is better than every other peer losing all its traffic while one peer is stuck.

- **Register Upstream Connection Error Callback** Provide a function to call when there is error with an upstream connection.
    - A default upstream connection error callback will be registered during instantiation.
        - Print a message to indicate that an upstream connection has error, with the necessary details.
- **Register Downstream Connection Error Callback** Provide a function to call when there is error with a downstream connection.
    - A default downstream connection error callback will be registered during instantiation.
        - Print a message to indicate that a downstream connection has error, with the necessary details.
- **Register Upstream New Connection Callback** Provide a function to call, with the connection, when a new upstream connection is established.
    - Default is None, meaning not to call any function.
    - If it fails, follow Callbacks: the connection is kept, and Establish Connection still returns it.
- **Register Downstream New Connection Callback** Provide a function to call, with the connection, when a new downstream connection is accepted.
    - Default is None, meaning not to call any function.
    - If it fails, follow Callbacks: the connection is kept.
- **Exceptions Related to a Connection**
    - Handling the following exception types:
        - Peer dropped connection.
        - Failure to send data to a connection.
        - Failure to receive data from a connection.
        - Receiving a message whose length is greater than the maximum payload length. The payload is not read.
        - The connection is too slow (see Sending Data).
    - Action depends on whether it is an upstream connection or a downstream connection.
        - If it is an upstream connection:
            - Remove the upstream connection from the list of upstream connection.
            - Call the upstream connection error callback with the connection details and error.
            - Close the upstream connection.
        - If it is a downstream connection:
            - Remove the downstream connection from the list of downstream connections.
            - Call the downstream connection error callback with the connection details and error.
            - Close the downstream connection.
    - If the connection error callback fails, follow Callbacks: the connection is still removed from its list and closed.
    - Closing a failed connection discards any data not sent yet on it, rather than waiting to send it.

- **Close** Stop accepting connections on every IP/port, and close every upstream and downstream connection.
    - Close the connections before waiting for the listening sockets to finish closing: waiting may otherwise never end while accepted connections are still open.
    - A connection that arrives while or after the node closes is closed at once, not added to a list: one accepted just before Close but handled only after it, or one Establish Connection finishes opening after Close has started (Establish Connection then fails with a connection error). Close did not see it, so nothing else would close it, and waiting for the listening sockets would never end.
    - Data still waiting in a send queue, or not yet taken by the operating system, is discarded, not sent: Close does not wait for slow peers.
    - Afterwards the lists of IP/port and of connections are empty.
    - Closing the node itself is not an error, and does not call the connection error callbacks. (The peers at the other end of the connections do see them dropped.)
    - A closed node is not meant to be used again: to accept or establish connections after Close, create a new instance. What happens if a closed node is used again is not defined. (Clients differ: a client can Connect again after Close, see Base Layer Client Class.)
- **Close Connection** Close one upstream or downstream connection on purpose, keeping the node and its other connections.
    - Parameters:
        - The connection to close: one of the node's upstream or downstream connections.
    - Remove the connection from its list, then close it. Data not sent yet on it is discarded, as in Close.
    - Closing a connection on purpose is not an error, and does not call the connection error callbacks. (The peer at the other end does see it dropped.)
    - If the connection is not one of the node's connections (e.g. it was closed or failed already), do nothing.
    - A connection must be closed through Close Connection or Close, not by closing the connection itself. The node would not know it is closed: it would keep it in its lists, as if it were still open, until a send to it failed, and then report it as a connection error.
- **Internal Data** Keep the following lists:
    - a list of IP/port that it is accepting connections on.
    - a list of upstream connections.
    - a list of downstream connections.

## Base Layer Client Class

Functionalities

- **Instantiation** Create an instance of the class
- **Connect** Create a TCP connection on an IP address and a port number.
    - The default IP address and port number is the same as the Base Layer Node Class.
    - If the client is already connected, report an error and change nothing: the existing connection is kept, and no new one is made.
        - This includes two Connects made at the same time: only one of them connects.
        - To connect again, Close first. Once the connection has been lost (see Exceptions Related to the Connection), the client is no longer connected and can Connect again.
- **Send** Send data to the TCP connection established.
    - If the client is not connected, report an error to the caller and send nothing.
    - If the data is longer than the maximum payload length, report an error to the caller and send nothing. This is not a connection error: the connection is kept, and the connection error callback is not called.
    - Send waits until the data has been handed to the operating system, so a caller sending many messages is slowed down to the speed the node takes them at. The client has a single connection, so this holds up only its own caller.
    - If the send timeout passes before the data has been handed over, the connection has failed: report it as a failure to send (see Exceptions Related to the Connection).
- **Register Receive Callback** Provide a function to call when data are received.
    - Default is None, meaning not to call any function.
- **Action Upon Receiving of Data** Call the given function, if any, with the data as one of the parameters.
    - If the function fails, follow Callbacks (see above): log it and carry on with the next data. The connection is kept, and this is not a connection error.
- **Register Connection Error Callback** Provide a function to call when there is error with the connection.
    - A default connection error callback will be registered during instantiation.
        - Print a message to indicate that the connection has error, with the necessary details.
- **Exceptions Related to the Connection**
    - Handling the following exception types:
        - Peer dropped connection.
        - Failure to send data to the connection.
        - Failure to receive data from the connection.
        - Receiving a message whose length is greater than the maximum payload length. The payload is not read.
    - Action:
        - Forget the connection, so that the client is no longer connected.
        - Call the connection error callback with the connection details and error.
        - Close the connection.
        - If it was a failure to send, also report the failure to the caller of Send.
    - If the connection error callback fails, follow Callbacks: the connection is still forgotten and closed, and a failure to send is still reported to the caller of Send as that failure.
    - Closing the client itself is not an error, and does not call the connection error callback.
- **Close** Close the connection, if any. The client is then no longer connected.
    - Data already sent is still delivered: Close waits for the node to take it, but at most the close timeout. Whatever the node has not taken by then is discarded, so Close never hangs on a node that has stopped reading.
        - It does not discard at once, as the node's Close does: a client often sends its last message just before closing, and that message must still arrive.
        - The close timeout is shorter than the send timeout: a node that is working takes the last data almost at once, so a long wait only delays a client being closed (e.g. with Ctrl-C) on a node that has stopped reading.

## bl_server Script

- Create an instance of Base Layer Node Class.
- Connect to zero or more upstream bl_server.
    - parameter --upstream ip:port (default: None)
- Accepting connection from zero or more downstream client.
    - parameter --listen ip:port (default: None)
    - If an address it listens on can be reached from other machines (anything but a loopback address such as 127.0.0.1 or ::1, or the name localhost), log a warning that anyone who can reach that address can use the bus, which has no authentication (see Trust Model). Listen on it anyway: this is allowed, e.g. on a trusted private network.
        - Any other host name counts as reachable: telling would mean resolving it.
- Reconnect to upstream servers.
    - parameter --reconnect true|false (default: false)
    - With --reconnect false:
        - Exit with an error message and exit status 1 if it cannot connect to an upstream server.
        - Losing an upstream connection later is not a reason to exit; the server keeps serving its other connections, but does not connect to that upstream server again.
    - With --reconnect true, each upstream server given with --upstream is handled on its own:
        - If it cannot be connected to, at start-up or later, log a warning, wait the reconnect delay, and try again. Keep trying until it succeeds or the server is stopped.
        - If its connection is lost, log a warning, wait the reconnect delay (the shortest one, since the last attempt succeeded), and try to connect again as above.
        - The server keeps serving its other connections throughout. It starts listening (--listen) straight away, without waiting for its upstream servers.
        - Messages that would have gone over a lost connection while it was down are not kept or resent: they are lost, as on any lost connection.
        - Reconnecting re-creates the same link to the same address. The Base Layer has no cycle prevention, so if the servers were linked differently while it was down, reconnecting can close a cycle; `bus_check` (`bus-check.md`) can find it.
    - Stopping the server (e.g. Ctrl-C) stops any attempt in progress and any waiting before the next one.
- Exit with an error message and exit status 1 if it cannot listen on an address (e.g. the address is already in use), whatever --reconnect is.
- Print logging information
    - parameter --debug true|false (default: false)

## bl_client Script

- Create an instance of Base Layer Client Class.
- Connect to an upstream bl_server.
    - parameter --upstream ip:port (default: as per default in Base Layer Client Class)
- Send messages.
    - parameter --message "message to be sent" (default: None, don't send any message)
    - parameter --repeat-count n (default: 1)
    - parameter --repeat-interval t (default: 1 sec)
- Listen for any received message.
    - print the message received with an optional time stamp
        - parameter --time-stamp true|false (default: true)
- Exit with an error message and exit status 1 if it cannot connect to the bl_server, or if the connection to the bl_server is lost, including while still sending messages.
- Print logging information
    - parameter --debug true|false (default: false)

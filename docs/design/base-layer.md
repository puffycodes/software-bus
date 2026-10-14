# Base Layer Design

## Definitions

- **Maximum payload length** The largest payload a single message can carry, 16 MiB (16777216 bytes). See "Base Layer Wire Format" in `data-format.md`.
- **Send timeout** 30 seconds. The longest time data may wait to be handed to the operating system for one connection.
- **Send queue limit** 64 MiB (four times the maximum payload length). The most data a node keeps waiting to be sent on one connection.
- **Reconnect delay** How long a server waits before trying an upstream connection again. The first delay is 1 second, and each failed attempt in a row doubles it, up to 60 seconds. Each delay is multiplied by a random factor between 0.5 and 1, so that servers that lost the same upstream node do not all try again at the same moment. The delay goes back to 1 second once a connection succeeds.

## Base Layer Node Class

Functionalities

- **Instantiation** Create an instance of the class.

- **Accept Connection** Start accepting TCP connections from downstream instances on a given IP address (default 127.0.0.1) and a given port (default 8787).
    - The instance can accept connections on multiple IP/port pairs.
    - The instance will not be accepting connections on any IP/port when it is instantiated.
    - If the instance is already accepting connections on the given IP/port, this will do nothing.
- **Establish Connection** Create a TCP connection to an upstream instance.

- **Register Upstream Receive Callback** Provide a function to call when data are received from any of the upstream connections.
    - A default upstream receive callback will be registered during instantiation.
        - The default upstream receive callback will re-send the data received from a upstream connection to all the downstream connections, as well as all the upstream connections, other than itself.
- **Register Downstream Receive Callback** Provide a function to call when data are received from any of the downstream connections.
    - A default downstream receive callback will be registered during instantiation.
        - The default downstream receive callback will re-send the data received from a downstream connection to all the upstream connections, as well as all the downstream connections, other than itself.
- **Action Upon Receiving of Data** Action depends on the connection that the data is received from.
    - If the data is received from an upstream connections, call the upstream receive callback function with the data as a parameter.
    - If the data is received from a downstream connections, call the downstream receive callback function with the data as a parameter.
    - If the callback fails (raises an error), log the error with its details and carry on with the next data. The connection is kept, and this is not a connection error.
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
- **Register Downstream New Connection Callback** Provide a function to call, with the connection, when a new downstream connection is accepted.
    - Default is None, meaning not to call any function.
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
    - Closing a failed connection discards any data not sent yet on it, rather than waiting to send it.

- **Close** Stop accepting connections on every IP/port, and close every upstream and downstream connection.
    - Close the connections before waiting for the listening sockets to finish closing: waiting may otherwise never end while accepted connections are still open.
    - Data still waiting in a send queue, or not yet taken by the operating system, is discarded, not sent: Close does not wait for slow peers.
    - Afterwards the lists of IP/port and of connections are empty.
    - Closing the node itself is not an error, and does not call the connection error callbacks. (The peers at the other end of the connections do see them dropped.)
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
    - If the function fails (raises an error), log the error with its details and carry on with the next data. The connection is kept, and this is not a connection error.
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
    - Closing the client itself is not an error, and does not call the connection error callback.
- **Close** Close the connection, if any. The client is then no longer connected.

## bl_server Script

- Create an instance of Base Layer Node Class.
- Connect to zero or more upstream bl_server.
    - parameter --upstream ip:port (default: None)
- Accepting connection from zero or more downstream client.
    - parameter --listen ip:port (default: None)
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

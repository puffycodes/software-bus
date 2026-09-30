# Base Layer Design

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
    - Action depends on whether it is an upstream connection or a downstream connection.
        - If it is an upstream connection:
            - Remove the upstream connection from the list of upstream connection.
            - Call the upstream connection error callback with the connection details and error.
            - Close the upstream connection.
        - If it is a downstream connection:
            - Remove the downstream connection from the list of downstream connections.
            - Call the downstream connection error callback with the connection details and error.
            - Close the downstream connection.

- **Close** Stop accepting connections on every IP/port, and close every upstream and downstream connection.
    - Close the connections before waiting for the listening sockets to finish closing: waiting may otherwise never end while accepted connections are still open.
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
- **Send** Send data to the TCP connection established.
- **Register Receive Callback** Provide a function to call when data are received.
    - Default is None, meaning not to call any function.
- **Action Upon Receiving of Data** Call the given function, if any, with the data as one of the parameters.
- **Register Connection Error Callback** Provide a function to call when there is error with the connection.
    - A default connection error callback will be registered during instantiation.
        - Print a message to indicate that the connection has error, with the necessary details.
- **Exceptions Related to the Connection**
    - Handling the following exception types:
        - Peer dropped connection.
        - Failure to send data to the connection.
        - Failure to receive data from the connection.
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
- Exit with an error message and exit status 1 if it cannot connect to an upstream server or cannot listen on an address (e.g. the address is already in use).
    - Losing an upstream connection later is not a reason to exit; the server keeps serving its other connections.
- Print logging information
    - parameter --debug true|false (default: false)

## bl_client Script

- Create an instance of Base Layer Client Class.
- Connect to an upstream bl_server.
    - parameter --upstream ip:port (default: as per default in Base Layer Node Class)
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

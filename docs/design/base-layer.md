# Base Layer Design

## Base Layer Class

Funcionalities

- **Instantiation** Create an instance of the class.
- **Accept Connection** Start accepting TCP connections from downstream instances on a given IP address (default 127.0.0.1) and a give port (default 8787).
    - The instance can accept connections on multiple IP/port pairs.
    - The instance will not be accepting connections on any IP/port when it is instantiate.
    - If the instance is already accepting connections on the given IP/port, this will do nothing.
- **Establish Connection** Create a TCP connection to an upstream instance.
- **Register Upstream Receive Callback** Provide a function to call when data are received from any of the upstream connections.
    - A default upstream receive callback will be registerd during instantiation.
        - The default upstream receive callback will re-send the data received from a upstream connection to all the downstream connections.
- **Register Downstream Receive Callback** Provide a function to call when data are received from any of the downstream connections.
    - A default downstream receive callback will be registerd during instantiation.
        - The default downstream receive callback will re-send the data received from a downstream connection to all the upstream connections, as well as all the downstream connections, other than itself.
- **Action Upon Receiving of Data** Actions depends on the connection that the data is received from.
    - If the data is received from an upstream connections, call the upstream receive callback function with the data as a parameter.
    - If the data is received from a downstream connections, call the downstream receive callback function with the data as a parameter.
- **Internal Data** Keep the following lists:
    - a list of IP/port that it is accepting connections on.
    - a list of upstream connections.
    - a list of downstream connections.

## Base Layer Client Class

Functionalities

- **Instantiation** Create an instance of the class
- **Connect** Create a TCP connection on an IP address and a port number.
    - The default IP address and port number is the same as the Base Layer Class.
- **Send** Send data to the TCP connection established.
- **Register Receive Callback** Provide a function to call when data are received.
    - Default is None, meaning not to call any function.
- **Action Upon Receiving of Data** Call the given function, if any, with the data as one of the parameters.

## bl_server Script

- Create an instance of Base Layer Class.
- Connect to zero or more upstream bl_server.
    - parameter --upstream ip:port (default: None)
- Accepting connection from zero or more downstream client.
    - parameter --listen ip:port (default: None)

## bl_client Script

- Create an instance of Base Layer Client Class.
- Connect to an upstream bl_server
    - parameter --upstream ip:port (default: as per default in Base Layer Class)
- Send messages.
    - parameter --message "message to be send" (default: None, don't send any message)
    - parameter --repeat-count n (default: 1)
    - parameter --repeat-interval t (default: 1 sec)
- Listen for any receieved message.
    - print the message received with an optional time stamp
        - parameter --time-stamp true|false (default: true)

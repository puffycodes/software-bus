# Base Layer Design

## Base Layer Class

Funcionalities

- **Instantiation** Create an instance of the class.
- **Accept Connection** Start accepting TCP connections from downstream instances on a given IP address (default 127.0.0.1) and a give port (default 8787).
    - The instance can accept connections on multiple IP/port pairs.
    - The instance will not be accepting connections on any IP/port when it is instantiate.
    - If the instance is already accepting connections on the given IP/port, this will do nothing.
- **Establish Connection** Create a TCP connection to an upstream instance.
- **Action Upon Receiving of Data** Actions depends on the connection that the data is received from.
    - If the data is received from a downstream connections, re-send it to all the upstream connections, as well as all the downstream connections, other than itself.
    - If the data is received from a upstream connections, re-send it to all the downstream connections.
- **Internal Data** Keep the following lists:
    - a list of IP/port that it is accepting connections on.
    - a list of upstream connections.
    - a list of downstream connections.

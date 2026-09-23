# Publish and Subscribe Design

## Definitions

- **Subject** A string with "." separators.
- **Content** Payload of a message associates with the subject.

## Messages

- **Subscription** A message to indicate the interest in a subject. Content of the message include:
    - A field to indicate whether to subscribe or unsubscribe from the subject.
        - subscribe means the connection that send the message wants to receive future messages tagged with the subject.
        - unsubscribe means the connection that send the message no longer wants to receive messages tagged with the subject.
- **Publish*** A message with a subject and a payload.

## Publish and Subscribe Node Class

- Use Base Layer Node class for the sending and receiving of data.
    - Register the necessary upstream and downstream receive callbacks to achieve its job.

- **Instantiation** Create an instance of the class
- **Message Processing**
    - Upon receiving of a subscription message with subscribe content.
        - If the message comes from a downstream connection:
            - Add the connection to the downstream connection list tagged to the subject.
            - Send a subscription message to all other downstream connections, except the one from which is message is received from.
            - Send a subscription message to all upstream connections, if any.
        - If the message comes from an upstream connection
            - Add the connection to the upstream connection list tagged to the subject.
            - Send a subscription message to all downstream connections, if any.
            - Send a subscription message to all other upstream connections, except the one from which the message is received from.
    - Upon receiving of a subscription message with unsubscribe content.
        - If the message comes from a downstream connection:
            - Remove the connection from the downstream connection list tagged to the subject.
        - If the message comes from an upstream connection:
            - Remove the connection from the upstream connection list tagged to the subject.
        - In both scenarios, if the resulted downstream connection list **and** the upstream connection list tagged to the subject is empty, send a unscribe message to all the downstream and upstream connections.
    - Upon receiving of a publish message.
        - Send a publish message to all the connections in the following lists that are tagged to the subject.
            - The downstream connection list.
            - The upstream connection list.

## Publish and Subscribe Client Class

- Use Base Layer Client class for the sending and receiving of data.
    - Register the necessary receive callback to achieve its job

- **Instantiation** Create an instance of the class
- **Subscribe** Send a subscription message to the upstream node to subscribe or unsubscribe from a given subject.
- **Publish** Send a publish message to the upstream node with the given subject and payload.
- **Register a Subscribe Callback** Provide a function to call when a subscription message is received.
    - The default subscribe callback will be None, which means not to call any function.
- **Register a Publish Callback** Provide a function to call when a publish message is received.
    - The default publish callback will be None, which means not to call any function.
- **Message Processing**
    - Upon receiving a subscription message, call the subscribe callback with the subject and a subscribe state as parameters.
        - The subscribe state will be true if the content of the message is subscribe, and false if the content of the message is unsubscribe.
    - Upon receiving a publish message, call the publish callback with the subject and the payload as the parameters.

## ps_server Script

- Create an instance of the Publish and Subscribe Node Class.
- Connect to zero or more upstream server.
    - parameter --upstream ip:port (default: None)
- Accepting connection from zero or more downstream client.
    - parameter --listen ip:port (default: None)
- Print logging information
    - parameter --debug true|false (default: false)

## ps_subscribe Script

- Create an instance of the Publish and Subscribe Client Class.
- Connect to an upstream server.
    - parameter --upstream ip:port (default: as per default in Base Layer Node Class)
- Subscribe to one or more subjects
    - parameter --subject "<subject_1>,<subject_2>,..." (required)
    - print the subject and the payload received with the publish message with an optional time stamp
        - parameter --time-stamp true|false (default: true)
- Print logging information
    - parameter --debug true|false (default: false)

## ps_publish Script

- Create an instance of the Publish and Subscribe Client Class.
- Connect to an upstream server.
    - parameter --upstream ip:port (default: as per default in Base Layer Node Class)
- Publish messages.
    - parameter --subject "subject to publish to" (required)
    - parameter --message "message to publish" (required)
    - parameter --repeat-count n (default: 1)
    - parameter --repeat-interval t (default: 1 sec)
- Print logging information
    - parameter --debug true|false (default: false)

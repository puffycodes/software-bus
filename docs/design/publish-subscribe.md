# Publish and Subscribe Design

## Definitions

- **Subject** A string with "." separators. A subject used in a Subscription message may be a literal subject or a pattern containing "*" wildcard sub-strings, as defined in `subject-matcher.md`. A subject used in a Publish message is always literal (no wildcards).
- **Content** Payload of a message associates with the subject.
- **Subject Matcher** The String Pattern Matcher from `subject-matcher.md`, used throughout this document to test whether a published subject matches a subscribed subject: the subscribed subject (which may contain wildcards) is the *target subject*, and the published subject is the *given subject*.

## Messages

- **Subscription** A message to indicate the interest in a subject. Content of the message include:
    - A field to indicate whether to subscribe or unsubscribe from the subject.
        - subscribe means the connection that send the message wants to receive future messages tagged with the subject.
        - unsubscribe means the connection that send the message no longer wants to receive messages tagged with the subject.
- **Publish*** A message with a subject and a payload.

## Publish and Subscribe Node Class

- Use Base Layer Node class for the sending and receiving of data.
    - Register the necessary upstream and downstream receive callbacks to achieve its job.
    - Register the necessary upstream and downstream connection error callbacks.

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
        - In both scenarios, if the resulted downstream connection list **and** the upstream connection list tagged to the subject is empty, send a unsubscribe message to all the downstream and upstream connections.
    - Upon receiving of a publish message.
        - Connection lists are tagged by the subscribed subject, which may be a pattern, while the publish message carries a literal subject — so a tag is not looked up by exact match. For each of the following lists, use the Subject Matcher to test the published subject against every tagged subject, and collect the connections under every tagged subject that matches.
            - The downstream connection list.
            - The upstream connection list.
        - Send a publish message to each collected connection exactly once, even if it is tagged under more than one matching subject (e.g. a connection subscribed to both "a.b" and "a.*" must still only receive one copy of a publish to "a.b").
- **Exception Handling**
    - When error occurs on a connection, remove the connection from every subject and propagate unsubscribes.

## Publish and Subscribe Client Class

- Use Base Layer Client class for the sending and receiving of data.
    - Register the necessary receive callback to achieve its job

- **Instantiation** Create an instance of the class
- **Subscribe**
    - Parameters:
        - subject to subscribe to
        - a function that is the subscription callback
            - the function take the following parameters:
                - the subject matched to
                - the actual subject in the publish message received
                - the payload in the publish message
    - Register the subscription callback under the given subject.
        - A subject can have multiple subscription callbacks.
    - If this is the first subscription to the given subject, send a subscription message to the upstream node to subscribe the given subject.
- **Unsubscribe**
    - Parameters:
        - subject to unsubscribe from
        - the corresponding subscription callback to remove
    - Remove the subscription callback from the given subject in the register.
    - If the particular subject no long has any subscription callback attached to it, send an unsubscription message to the upstream node to unsubscribe from the given subject.
- **Publish**
    - Parameters:
        - subject to publish to
        - payload of the publish message
    - Send a publish message to the upstream node with the given subject and payload.
- **Message Processing**
    - Upon receiving a subscription message, log the subscription message.
        - Note: The subscription message currently has no use to a Client Node. If some use case araises, the logicall move is to provide a callback here. This will not be implemented yet.
    - Upon receiving a publish message:
        - Using the Subject Matcher, look up **all** the subscribed subjects that match the published subject, and their corresponding list of subscription callbacks.
        - Call every callbacks using the matched (subscribed) subject, the actual (published) subject and the payload as the parameters.

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
    - print the matched subject, the actual subject and the payload received with the publish message with an optional time stamp
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

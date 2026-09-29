# Publish and Subscribe Design

## Definitions

- **Subject** A string with "." separators. A subject used in a Subscription message may be a literal subject or a pattern containing "*" wildcard sub-strings, as defined in `subject-matcher.md`. A subject used in a Publish message is always literal (no wildcards).
- **Content** Payload of a message associates with the subject.
- **Subject Matcher** The String Pattern Matcher from `subject-matcher.md`, used throughout this document to test whether a published subject matches a subscribed subject: the subscribed subject (which may contain wildcards) is the *target subject*, and the published subject is the *given subject*.
- **Topology** Nodes must be connected as a tree (no cycles). The routing rules below never send a message back to the connection it arrived on, which is enough to prevent duplicates and loops in a tree, but not in a graph with cycles.
- **Subscribers of a subject** On a node, the connections tagged to a subject in either the downstream or the upstream connection list.

## Messages

- **Subscription** A message to indicate the interest in a subject. Content of the message include:
    - A field to indicate whether to subscribe or unsubscribe from the subject.
        - subscribe means the connection that send the message wants to receive future messages tagged with the subject.
        - unsubscribe means the connection that send the message no longer wants to receive messages tagged with the subject.
- **Publish** A message with a subject and a payload.

## Publish and Subscribe Node Class

- Use Base Layer Node class for the sending and receiving of data.
    - Register the necessary upstream and downstream receive callbacks to achieve its job.
    - Register the necessary upstream and downstream connection error callbacks.
    - Register the necessary upstream and downstream new connection callbacks.

- **Instantiation** Create an instance of the class
- **New Connection**
    - When a new upstream or downstream connection is made, send it a subscription message with subscribe content for every subject that currently has subscribers, so that a node joining the tree late learns about existing subscriptions.
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
        - If the connection was not tagged to the subject, do nothing further.
        - Otherwise, decide for each other connection separately whether this node still wants messages for the subject from it: send an unsubscribe message to a connection when no connection other than that one remains a subscriber of the subject. That is:
            - If the subject has no subscribers left, send an unsubscribe message to every connection except the one the message came from.
            - If exactly one subscriber is left, send an unsubscribe message to that subscriber only (it is the only remaining interest, and it would not want its own messages back).
            - If two or more subscribers are left, send nothing.
        - Note: checking only whether the subject has no subscribers left is not enough. Two neighbouring nodes that each have a subscriber tag each other as subscribers, so neither would ever become empty and the subscription would never be torn down.
    - Upon receiving of a publish message.
        - Connection lists are tagged by the subscribed subject, which may be a pattern, while the publish message carries a literal subject — so a tag is not looked up by exact match. For each of the following lists, use the Subject Matcher to test the published subject against every tagged subject, and collect the connections under every tagged subject that matches.
            - The downstream connection list.
            - The upstream connection list.
        - Leave out the connection the publish message was received from, even if it is subscribed to the subject. Otherwise two neighbouring nodes that both have subscribers would send the same publish back and forth forever. (A client still sees its own publishes; see the client's Publish below.)
        - Send a publish message to each remaining collected connection exactly once, even if it is tagged under more than one matching subject (e.g. a connection subscribed to both "a.b" and "a.*" must still only receive one copy of a publish to "a.b").
    - Upon receiving a malformed message (see `data-format.md`), log it and ignore it. The connection is kept.
- **Exception Handling**
    - When error occurs on a connection, remove the connection from every subject, applying the unsubscribe rules above for each subject as if the connection had sent an unsubscribe message.

## Publish and Subscribe Client Class

- Use Base Layer Client class for the sending and receiving of data.
    - Register the necessary receive callback to achieve its job

- **Instantiation** Create an instance of the class
- **Register Connection Error Callback** Provide a function to call when there is error with the connection to the upstream node. This is passed straight to the Base Layer Client, with the same default.
- **Subscribe**
    - Parameters:
        - subject to subscribe to
        - a function that is the subscription callback
            - the function take the following parameters:
                - the subject matched to
                - the actual subject in the publish message received
                - the payload in the publish message
    - If the subject is too long to encode (see `data-format.md`), report an error and change nothing.
    - Register the subscription callback under the given subject.
        - A subject can have multiple subscription callbacks.
    - If this is the first subscription to the given subject, send a subscription message to the upstream node to subscribe the given subject.
- **Unsubscribe**
    - Parameters:
        - subject to unsubscribe from
        - the corresponding subscription callback to remove
    - Remove the subscription callback from the given subject in the register.
    - If the particular subject no longer has any subscription callback attached to it, send an unsubscription message to the upstream node to unsubscribe from the given subject.
- **Publish**
    - Parameters:
        - subject to publish to
        - payload of the publish message
    - Send a publish message to the upstream node with the given subject and payload.
    - If the client itself has subscribed subjects that match the given subject, call their subscription callbacks too, the same way as for a received publish message. The node does not send a publish back to the connection it came from, so this is how a client receives its own publishes.
- **Message Processing**
    - Upon receiving a subscription message, log the subscription message.
        - Note: The subscription message currently has no use to a Client Node. If some use case arises, the logical move is to provide a callback here. This will not be implemented yet.
    - Upon receiving a malformed message (see `data-format.md`), log it and ignore it. The connection is kept.
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
- Exit with an error message and exit status 1 if the connection to the server is lost.
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

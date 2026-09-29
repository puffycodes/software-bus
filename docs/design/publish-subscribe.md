# Publish and Subscribe Design

## Definitions

- **Subject** A string with "." separators. A subject used in a Subscription message may be a literal subject or a pattern containing "*" wildcard sub-strings, as defined in `subject-matcher.md`. A subject used in a Publish message is always literal (no wildcards).
- **Content** Payload of a message associates with the subject.
- **Subject Matcher** The String Pattern Matcher from `subject-matcher.md`, used throughout this document to test whether a published subject matches a subscribed subject: the subscribed subject (which may contain wildcards) is the *target subject*, and the published subject is the *given subject*.
- **Topology** Nodes must be connected as a tree (no cycles). The routing rules below never send a message back to the connection it arrived on, which is enough to prevent duplicates and loops in a tree, but not in a graph with cycles. Nodes enforce this themselves: a node refuses a new connection that would close a cycle (see Cycle Prevention).
- **Subscribers of a subject** On a node, the connections tagged to a subject in either the downstream or the upstream connection list.
- **Node ID** 16 random bytes that identify a node, generated when the node is created (e.g. a random UUID).
- **Node connection** On a node, a connection from which a Hello message has been received, i.e. a connection to another node. A connection that never sends a Hello message is a connection to a client.
- **Pending connection** On a node, a connection that this node established (to an upstream node) and has not yet accepted, because the cycle check for it has not finished. Only the node that establishes a connection sees it as pending; the node that accepts it treats it like any other connection from the start.
- **Active connection** Any connection that is not pending. Unless stated otherwise, "connection" in the rules of this document means an active connection: pending connections are left out of every set of connections that messages are sent to.
- **Check timeout** 5 seconds.

## Messages

- **Subscription** A message to indicate the interest in a subject. Content of the message include:
    - A field to indicate whether to subscribe or unsubscribe from the subject.
        - subscribe means the connection that send the message wants to receive future messages tagged with the subject.
        - unsubscribe means the connection that send the message no longer wants to receive messages tagged with the subject.
- **Publish** A message with a subject and a payload.
- **Hello** A message a node sends on every new connection, carrying its Node ID. Used only for cycle prevention.
- **Reachability Query** A message asking whether a given Node ID can be reached through the node that receives it. Used only for cycle prevention, between nodes.
- **Reachability Reply** The answer to a Reachability Query: found, not found, or unknown. Used only for cycle prevention, between nodes.

## Publish and Subscribe Node Class

- Use Base Layer Node class for the sending and receiving of data.
    - Register the necessary upstream and downstream receive callbacks to achieve its job.
    - Register the necessary upstream and downstream connection error callbacks.
    - Register the necessary upstream and downstream new connection callbacks.

- **Instantiation** Create an instance of the class, with a new Node ID.
- **Establish Connection** Connect to an upstream node, as a Base Layer Node does, then run the cycle check for the new connection (see Cycle Prevention).
    - Only return once the check has finished and the connection is accepted.
    - If the connection is refused, close it and report an error to the caller that says why.
- **New Connection**
    - When a new upstream or downstream connection is made, first send it a Hello message with this node's Node ID.
    - For an accepted (downstream) connection: then straight away send it a subscription message with subscribe content for every subject that currently has subscribers, so that a node joining the tree late learns about existing subscriptions.
    - For an established (upstream) connection: the connection is pending until the cycle check accepts it. Send the subscription messages above only once it is accepted.
- **Message Processing**
    - Upon receiving a Hello, Reachability Query or Reachability Reply message, follow Cycle Prevention.
    - Upon receiving a subscription or publish message on a pending connection, keep it, in the order received, without processing it. Process the kept messages in order if the connection is accepted; discard them if it is refused.
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
    - When error occurs on a pending connection, the cycle check for it ends, and Establish Connection reports the error to its caller.
- **Close** Close the Base Layer Node (which stops accepting connections and closes every connection), stop answering any reachability queries still in progress, and forget every subscription.

## Cycle Prevention

A node refuses a new connection that would close a cycle, so that the nodes always form a tree. A cycle is formed exactly when the node being connected to can already be reached, through the existing connections, from the node that establishes the connection. Only the node that **establishes** the connection (called the *checking node* below) runs the check and decides; the node that accepts the connection takes no part in the decision.

### Checking a new connection

When node A establishes a connection to node B:

1. The connection is pending. A sends a Hello message on it, like on any new connection.
2. A waits, up to the check timeout, for a Hello message from B. If none arrives, refuse the connection: the peer did not identify itself as a Publish and Subscribe node (it may be a Base Layer Node, or a node that does not implement cycle prevention).
3. If B's Node ID is A's own Node ID, refuse the connection: it is a connection to itself.
4. If A already has an active node connection whose Node ID is B's, refuse the connection: A is already connected to that node.
5. Otherwise A sends a Reachability Query, with a new query ID and B's Node ID as the target, to every active node connection it has, and combines the replies (see Combining replies below):
    - If A has no active node connections, the result is not found.
    - **found:** refuse the connection: it would create a cycle.
    - **not found:** accept the connection.
    - **unknown**, or the check timeout passes before the result is known: wait a random delay between 0.2 and 2 seconds, then repeat this step with a new query ID. Give up after 5 attempts in all, and refuse the connection: whether it would create a cycle could not be determined.
6. When the connection is accepted, it becomes active:
    - Process the subscription and publish messages kept from it, in the order received.
    - Send it a subscription message for every subject that currently has subscribers (see New Connection).
7. When the connection is refused: close it, log the reason, and report it to the caller of Establish Connection.

A node checks one new connection at a time. If it establishes another connection while a check is still running, the new connection stays pending, and its check starts once the earlier one has finished. (Two connections checked at the same time by one node could each pass their check and still form a cycle together.)

A node is *checking* while it has a Reachability Query of its own waiting for replies (step 5). It is not checking while it waits for a Hello message, or during the random delay between attempts.

### Answering a query

When a node N receives a Reachability Query from connection S, apply the first rule that fits:

- If S is a pending connection of N: reply unknown to S if N is checking, otherwise reply not found. A pending connection is treated as not being there yet, even when N itself is the target, because it may still be refused.
- If the target is N's own Node ID, reply found to S.
- If N is checking a connection of its own, reply unknown to S (see Why "unknown" below).
- If N is already handling a query with the same query ID, reply unknown to S and log a warning: the same query arriving twice means there is already a cycle somewhere.
- Otherwise, send the query unchanged to every active node connection other than S, and combine their replies (see Combining replies). If there are none, reply not found to S. If the check timeout passes before the result is known, reply unknown to S.

In every case, reply to S as soon as the result is known, then forget the query. Replies that arrive after that are ignored.

### Combining replies

A node that sent a query to a set of connections combines their replies like this:

- As soon as any reply is found, the result is found.
- When every connection has replied: if any reply is unknown, the result is unknown; otherwise it is not found.
- A connection that fails before replying counts as not found. The part of the tree behind it is no longer connected, so the target cannot be reached through it.

### Why "unknown"

Two connections established at about the same time by different nodes could each pass their check and still form a cycle together. For example, if A–C and B–D are already connected, A connects to B while C connects to D. Each check only sees the other new connection as not there yet, so each sees no cycle.

A node that is checking therefore answers every query it receives with unknown:

- Any path that would use a pending connection passes through the node checking it. If the query reaches the accepting end first, that end sends it on over the connection, and so to the checking node.
- So a check that depends on another node's new connection gets unknown, and tries again after a random delay.
- Because a node is not checking during its own delay, one of the two checks soon finishes on its own. The other check, on its next attempt, sees the connection just accepted, and finds the cycle if there is one.

### Compatibility

- Every node in a tree must implement cycle prevention. A node that does not will not reply to queries, so checks through it time out and are refused.
- Clients take no part: they never send Hello messages, so nodes never send them queries. Clients from before this change log a Hello message as malformed and ignore it.
- The Base Layer has no cycle prevention: a tree of Base Layer Nodes must still be kept free of cycles by whoever connects them. The `bus_check` script (`bus-check.md`) can find cycles among the nodes running on a machine.

## Publish and Subscribe Client Class

- Use Base Layer Client class for the sending and receiving of data.
    - Register the necessary receive callback to achieve its job

- **Instantiation** Create an instance of the class
- **Register Connection Error Callback** Provide a function to call when there is error with the connection to the upstream node. This is passed straight to the Base Layer Client, with the same default.
- **Close** Close the connection to the upstream node, as the Base Layer Client does.
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
    - Upon receiving a Hello message (every node sends one on a new connection), ignore it without logging it as malformed. A client never sends a Hello message.
    - Upon receiving a Reachability Query or Reachability Reply message, log it and ignore it. Nodes do not send these to clients.
    - Upon receiving a publish message:
        - Using the Subject Matcher, look up **all** the subscribed subjects that match the published subject, and their corresponding list of subscription callbacks.
        - Call every callbacks using the matched (subscribed) subject, the actual (published) subject and the payload as the parameters.

## ps_server Script

- Create an instance of the Publish and Subscribe Node Class.
- Connect to zero or more upstream server.
    - parameter --upstream ip:port (default: None)
- Accepting connection from zero or more downstream client.
    - parameter --listen ip:port (default: None)
- Exit with an error message and exit status 1 if it cannot connect to an upstream server or cannot listen on an address (e.g. the address is already in use).
    - This includes an upstream connection refused by the cycle check (see Cycle Prevention), with the reason, e.g. that it would create a cycle.
    - Losing an upstream connection later is not a reason to exit; the server keeps serving its other connections.
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
- Exit with an error message and exit status 1 if it cannot connect to the server, or if the connection to the server is lost.
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
- Exit with an error message and exit status 1 if it cannot connect to the server, or if the connection to the server is lost while still publishing.
- Print logging information
    - parameter --debug true|false (default: false)

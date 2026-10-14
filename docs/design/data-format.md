# Data Format

This document defines all the data format used in this project. The data format should be followed to ensure compatibility between different implementations.

## Base Layer Wire Format

Every connection is framed the same way, regardless of what layer is on top of it:

```
<length: 4 bytes, big-endian><payload: length bytes>
```

- `length` is the number of bytes in `payload`, encoded as an unsigned 4-byte big-endian integer.
- `payload` is opaque to the base layer: it is handed unmodified to the receive callback, and to peers when relayed.
- `length` must not be greater than the **maximum payload length**, 16 MiB (16777216 bytes).
    - Every implementation uses the same limit, so that a message one node accepts is never refused by the next node it is relayed to.
    - A sender must not send a longer payload: it reports an error and sends nothing (see `base-layer.md`).
    - A receiver that reads a `length` greater than the limit must not read the payload. The connection has failed (see Exceptions Related to a Connection in `base-layer.md`): there is no way to find the start of the next message without reading the whole payload, and reading it is what the limit is there to prevent.

## Publish and Subscribe Wire Format

The pub/sub layer defines the contents of the base layer's `payload`. Every payload starts with a 1-byte message type tag, followed by a type-specific body:

| Tag    | Message            |
|--------|--------------------|
| `0x01` | Subscription       |
| `0x02` | Publish            |
| `0x03` | Hello              |
| `0x04` | Reachability Query |
| `0x05` | Reachability Reply |

Tags `0x03`–`0x05` are used only between nodes, for cycle prevention (see `publish-subscribe.md`).

### Subscription Message

```
<type: 0x01><state: 1 byte><subject_length: 2 bytes, big-endian><subject: subject_length bytes, utf-8>
```

- `state` is `0x01` to subscribe, `0x00` to unsubscribe.
- `subject` is the subject string, UTF-8 encoded, preceded by its length as an unsigned 2-byte big-endian integer.
    - A subject can therefore be at most 65535 bytes once UTF-8 encoded. An encoder must reject a longer subject with an error, rather than truncating it or producing a malformed message.
- An encoder sends nothing after the subject. A decoder ignores any bytes after it.

### Publish Message

```
<type: 0x02><subject_length: 2 bytes, big-endian><subject: subject_length bytes, utf-8><payload: remaining bytes>
```

- `subject` is encoded the same way as in the Subscription Message.
- `payload` is the published content: every byte after the subject, taken as-is with no further framing.
- The whole Publish Message, including its tag and subject, is the base layer's `payload`, so it must fit the maximum payload length. The longest `payload` a publish can carry is therefore 16777216 − 3 − the length of the encoded subject.

### Hello Message

```
<type: 0x03><node_id: 16 bytes>
```

- `node_id` is the sending node's Node ID: 16 random bytes generated when the node is created (e.g. a random UUID).

### Reachability Query Message

```
<type: 0x04><query_id: 16 bytes><target_node_id: 16 bytes>
```

- `query_id` is 16 random bytes, new for every query sent by the node that starts the check. Nodes that pass the query on keep the same `query_id`.
- `target_node_id` is the Node ID being looked for.

### Reachability Reply Message

```
<type: 0x05><query_id: 16 bytes><result: 1 byte>
```

- `query_id` is the `query_id` of the query being answered.
- `result` is one of:

| Value  | Meaning   |
|--------|-----------|
| `0x00` | not found |
| `0x01` | found     |
| `0x02` | unknown   |

### Malformed Messages

A pub/sub payload is malformed if any of the following hold:

- it is empty, or its type tag is not one listed above;
- a Subscription Message's `state` is missing or is not `0x00` or `0x01`;
- it is shorter than its `subject_length` says, or the subject is not valid UTF-8;
- a Hello, Reachability Query or Reachability Reply Message is not exactly 17, 33 or 18 bytes long respectively (including the tag);
- a Reachability Reply Message's `result` is not `0x00`, `0x01` or `0x02`.

How a receiver handles a malformed message is defined in `publish-subscribe.md`.

## Request and Reply Wire Format

The request/reply layer defines the contents of a Publish Message's `payload`. It adds no message type tag: what a payload means is given by the subject it is published to (see `request-reply.md`).

### Request Message

Published to a Derived Request Subject.

```
<reply_subject_length: 2 bytes, big-endian><reply_subject: reply_subject_length bytes, utf-8><request_payload: remaining bytes>
```

- `reply_subject` is the Derived Reply Subject the requestor listens on, encoded the same way as `subject` in the Subscription Message.
- `request_payload` is every byte after the reply subject, taken as-is with no further framing. It can be empty.

### Reply Message

Published to a reply subject. It has no framing of its own: the Publish Message's `payload` is the reply payload, taken as-is. It can be empty.

### Malformed Request Messages

A Request Message is malformed if it is shorter than 2 bytes, it is shorter than its `reply_subject_length` says, or the reply subject is not valid UTF-8.

How a receiver handles a malformed Request Message is defined in `request-reply.md`.

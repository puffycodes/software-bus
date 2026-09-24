# Data Format

This document defines all the data format used in this project. The data format should be followed to ensure compatibility between different implementations.

## Base Layer Wire Format

Every connection is framed the same way, regardless of what layer is on top of it:

```
<length: 4 bytes, big-endian><payload: length bytes>
```

- `length` is the number of bytes in `payload`, encoded as an unsigned 4-byte big-endian integer.
- `payload` is opaque to the base layer: it is handed unmodified to the receive callback, and to peers when relayed.

## Publish and Subscribe Wire Format

The pub/sub layer defines the contents of the base layer's `payload`. Every payload starts with a 1-byte message type tag, followed by a type-specific body:

| Tag    | Message      |
|--------|--------------|
| `0x01` | Subscription |
| `0x02` | Publish      |

### Subscription Message

```
<type: 0x01><state: 1 byte><subject_length: 2 bytes, big-endian><subject: subject_length bytes, utf-8>
```

- `state` is `0x01` to subscribe, `0x00` to unsubscribe.
- `subject` is the subject string, UTF-8 encoded, preceded by its length as an unsigned 2-byte big-endian integer.

### Publish Message

```
<type: 0x02><subject_length: 2 bytes, big-endian><subject: subject_length bytes, utf-8><payload: remaining bytes>
```

- `subject` is encoded the same way as in the Subscription Message.
- `payload` is the published content: every byte after the subject, taken as-is with no further framing.

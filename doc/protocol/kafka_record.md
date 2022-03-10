```
length: varint
attributes: int8
    bit 0~7: unused
timestampDelta: varlong
offsetDelta: varint
keyLength: varint
key: byte[]
valueLen: varint
value: byte[]
Headers => [Header]
```

```
headerKeyLength: varint
headerKey: String
headerValueLength: varint
Value: byte[]
```

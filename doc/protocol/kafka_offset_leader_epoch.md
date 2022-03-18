```
OffsetForLeaderEpoch Request (Version: 3) => replica_id [topics] 
  replica_id => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch leader_epoch 
      partition => INT32
      current_leader_epoch => INT32
      leader_epoch => INT32
```

```
OffsetForLeaderEpoch Response (Version: 3) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => error_code partition leader_epoch end_offset 
      error_code => INT16
      partition => INT32
      leader_epoch => INT32
      end_offset => INT64
```

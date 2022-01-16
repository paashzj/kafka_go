package codec

type OffsetCommitResp struct {
	BaseResp
	ThrottleTime int
	Topics       []*OffsetCommitTopicResp
}

type OffsetCommitTopicResp struct {
	Topic      string
	Partitions []*OffsetCommitPartitionResp
}

type OffsetCommitPartitionResp struct {
	PartitionId int
	ErrorCode   int16
}

func NewOffsetCommitResp(corrId int) *OffsetCommitResp {
	CommitResp := OffsetCommitResp{}
	CommitResp.CorrelationId = corrId
	return &CommitResp
}

func (o *OffsetCommitResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 8 {
		result += LenTaggedField + LenThrottleTime
	}
	if version == 2 {
		result += LenArray
	} else if version == 8 {
		result += varintSize(len(o.Topics) + 1)
	}
	for _, val := range o.Topics {
		if version == 2 {
			result += StrLen(val.Topic)
		} else if version == 8 {
			result += CompactStrLen(val.Topic)
		}
		if version == 2 {
			result += LenArray
		} else if version == 8 {
			result += varintSize(len(val.Partitions) + 1)
		}
		for range val.Partitions {
			result += LenPartitionId + LenErrorCode
			if version == 8 {
				result += LenTaggedField
			}
		}
		if version == 8 {
			result += LenTaggedField
		}
	}
	if version == 8 {
		result += LenTaggedField
	}
	return result
}

func (o *OffsetCommitResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 8 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 2 {
		idx = putArrayLen(bytes, idx, len(o.Topics))
	} else if version == 8 {
		idx = putCompactArrayLen(bytes, idx, len(o.Topics))
	}
	for _, topic := range o.Topics {
		if version == 2 {
			idx = putTopicString(bytes, idx, topic.Topic)
		} else if version == 8 {
			idx = putTopic(bytes, idx, topic.Topic)
		}
		if version == 2 {
			idx = putArrayLen(bytes, idx, len(topic.Partitions))
		} else if version == 8 {
			idx = putCompactArrayLen(bytes, idx, len(topic.Partitions))
		}
		for _, partition := range topic.Partitions {
			idx = putInt(bytes, idx, partition.PartitionId)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			if version == 8 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 8 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 8 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

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

func NewOffsetCommitResp(version int16, corrId int) *OffsetCommitResp {
	CommitResp := OffsetCommitResp{}
	CommitResp.CorrelationId = corrId
	return &CommitResp
}

func (o *OffsetCommitResp) BytesLength(version int16) int {
	// 4字节CorrId + 1字节TaggedField + 4字节 ThrottleTime
	// 1字节Topics长度 + n个Topic长度
	result := LenCorrId + LenTaggedField + LenThrottleTime + varintSize(len(o.Topics)+1)
	for _, val := range o.Topics {
		// TopicName长度
		result += CompactStrLen(val.Topic)
		// 1字节数组长度
		result += varintSize(len(val.Partitions) + 1)
		for range val.Partitions {
			result += LenPartitionId + LenErrorCode + LenTaggedField
		}
		result += LenTaggedField
	}
	return result + LenTaggedField
}

func (o *OffsetCommitResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	idx = putCompactArrayLen(bytes, idx, len(o.Topics))
	for _, topic := range o.Topics {
		idx = putTopic(bytes, idx, topic.Topic)
		idx = putCompactArrayLen(bytes, idx, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			idx = putInt(bytes, idx, partition.PartitionId)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			idx = putTaggedField(bytes, idx)
		}
		idx = putTaggedField(bytes, idx)
	}
	idx = putTaggedField(bytes, idx)
	return bytes
}

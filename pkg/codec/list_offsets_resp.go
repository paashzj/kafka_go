package codec

type ListOffsetResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
	OffsetTopics []*ListOffsetTopicResp
}

type ListOffsetTopicResp struct {
	Topic                string
	ListOffsetPartitions []*ListOffsetPartitionResp
}

type ListOffsetPartitionResp struct {
	PartitionId int
	ErrorCode   int16
	Timestamp   int64
	Offset      int64
	LeaderEpoch int
}

func NewListOffsetResp(version int16, corrId int) *ListOffsetResp {
	resp := ListOffsetResp{}
	resp.CorrelationId = corrId
	return &resp
}

func (o *ListOffsetResp) BytesLength(version int16) int {
	result := LenCorrId + LenThrottleTime + LenArray
	for _, val := range o.OffsetTopics {
		result += StrLen(val.Topic) + LenArray
		for range val.ListOffsetPartitions {
			result += LenPartitionId + LenErrorCode + LenTime + LenOffset + LenLeaderEpoch
		}
	}
	return result
}

func (o *ListOffsetResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	idx = putArrayLen(bytes, idx, len(o.OffsetTopics))
	for _, topic := range o.OffsetTopics {
		idx = putTopicString(bytes, idx, topic.Topic)
		idx = putArrayLen(bytes, idx, len(topic.ListOffsetPartitions))
		for _, p := range topic.ListOffsetPartitions {
			idx = putInt(bytes, idx, p.PartitionId)
			idx = putErrorCode(bytes, idx, p.ErrorCode)
			idx = putInt64(bytes, idx, p.Timestamp)
			idx = putInt64(bytes, idx, p.Offset)
			idx = putLeaderEpoch(bytes, idx, p.LeaderEpoch)
		}
	}
	return bytes
}

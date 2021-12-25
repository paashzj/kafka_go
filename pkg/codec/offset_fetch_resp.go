package codec

type OffsetFetchResp struct {
	BaseResp
	ThrottleTime  int
	ErrorCode     int16
	TopicRespList []*OffsetFetchTopicResp
}

type OffsetFetchTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetFetchPartitionResp
}

type OffsetFetchPartitionResp struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int
	Metadata    *string
	ErrorCode   int16
}

func NewOffsetFetchResp(version int16, corrId int) *OffsetFetchResp {
	fetchResp := OffsetFetchResp{}
	fetchResp.CorrelationId = corrId
	return &fetchResp
}

func (o *OffsetFetchResp) BytesLength(version int16) int {
	// 4字节CorrId + 1字节TaggedField + 4字节 ThrottleTime
	// 1字节Topics长度 + n个Topic长度
	// 2 字节ErrorCode
	result := LenCorrId + LenTaggedField + LenThrottleTime + varintSize(len(o.TopicRespList)+1)
	for _, val := range o.TopicRespList {
		// TopicName长度
		result += CompactStrLen(val.Topic)
		// 1字节数组长度
		result += varintSize(len(val.PartitionRespList) + 1)
		for _, val2 := range val.PartitionRespList {
			result += LenPartitionId + LenOffset + LenLeaderEpoch
			result += CompactNullableStrLen(val2.Metadata) + LenErrorCode + LenTaggedField
		}
		result += LenTaggedField
	}
	return result + LenErrorCode + LenTaggedField
}

func (o *OffsetFetchResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	for _, topic := range o.TopicRespList {
		idx = putTopic(bytes, idx, topic.Topic)
		idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		for _, partition := range topic.PartitionRespList {
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putOffset(bytes, idx, partition.Offset)
			idx = putLeaderEpoch(bytes, idx, partition.LeaderEpoch)
			idx = putMetadata(bytes, idx, partition.Metadata)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			idx = putTaggedField(bytes, idx)
		}
		idx = putTaggedField(bytes, idx)
	}
	idx = putErrorCode(bytes, idx, o.ErrorCode)
	idx = putTaggedField(bytes, idx)
	return bytes
}

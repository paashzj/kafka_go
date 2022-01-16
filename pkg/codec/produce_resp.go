package codec

type ProduceResp struct {
	BaseResp
	TopicRespList []*ProduceTopicResp
	ThrottleTime  int
}

type ProduceTopicResp struct {
	Topic             string
	PartitionRespList []*ProducePartitionResp
}

type ProducePartitionResp struct {
	PartitionId    int
	ErrorCode      int16
	Offset         int64
	Time           int64
	LogStartOffset int64
}

func NewProduceResp(corrId int) *ProduceResp {
	produceResp := ProduceResp{}
	produceResp.CorrelationId = corrId
	return &produceResp
}

func (p *ProduceResp) BytesLength(version int16) int {
	result := LenCorrId
	result += LenArray
	for _, val := range p.TopicRespList {
		result += StrLen(val.Topic)
		result += LenArray
		for range val.PartitionRespList {
			result += LenPartitionId + LenErrorCode + LenOffset
			result += LenTime + LenOffset
		}
	}
	return result + LenThrottleTime
}

func (p *ProduceResp) Bytes(version int16) []byte {
	bytes := make([]byte, p.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, p.CorrelationId)
	idx = putArrayLen(bytes, idx, len(p.TopicRespList))
	for _, topic := range p.TopicRespList {
		idx = putTopicString(bytes, idx, topic.Topic)
		idx = putArrayLen(bytes, idx, len(topic.PartitionRespList))
		for _, partition := range topic.PartitionRespList {
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			idx = putOffset(bytes, idx, partition.Offset)
			idx = putTime(bytes, idx, partition.Time)
			idx = putLogStartOffset(bytes, idx, partition.LogStartOffset)
		}
	}
	idx = putThrottleTime(bytes, idx, p.ThrottleTime)
	return bytes
}

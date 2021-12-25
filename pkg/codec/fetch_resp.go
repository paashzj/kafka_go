package codec

type FetchResp struct {
	BaseResp
	ThrottleTime   int
	ErrorCode      int16
	SessionId      int
	TopicResponses []*FetchTopicResp
}

type FetchTopicResp struct {
	Topic             string
	PartitionDataList []*FetchPartitionResp
}

type FetchPartitionResp struct {
	PartitionIndex      int
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions int64
	ReplicaData         int64
	RecordBatch         *RecordBatch
}

func NewFetchResp(version int16, corrId int) *FetchResp {
	resp := FetchResp{}
	resp.CorrelationId = corrId
	return &resp
}

func (f *FetchResp) BytesLength(version int16) int {
	result := LenCorrId + LenThrottleTime + LenErrorCode + LenFetchSessionId + LenArray
	for _, t := range f.TopicResponses {
		result += StrLen(t.Topic) + LenArray
		for _, p := range t.PartitionDataList {
			result += LenPartitionId + LenErrorCode + LenOffset + LenLastStableOffset + LenStartOffset
			result += LenAbortTransactions + LenReplicaId + LenMessageSize + p.RecordBatch.BytesLength()
		}
	}
	return result
}

func (f *FetchResp) Bytes(version int16) []byte {
	bytes := make([]byte, f.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, f.CorrelationId)
	idx = putThrottleTime(bytes, idx, f.ThrottleTime)
	idx = putErrorCode(bytes, idx, f.ErrorCode)
	idx = putInt(bytes, idx, f.SessionId)
	idx = putArrayLen(bytes, idx, len(f.TopicResponses))
	for _, t := range f.TopicResponses {
		idx = putString(bytes, idx, t.Topic)
		idx = putArrayLen(bytes, idx, len(t.PartitionDataList))
		for _, p := range t.PartitionDataList {
			idx = putInt(bytes, idx, p.PartitionIndex)
			idx = putErrorCode(bytes, idx, p.ErrorCode)
			idx = putInt64(bytes, idx, p.HighWatermark)
			idx = putInt64(bytes, idx, p.LastStableOffset)
			idx = putInt64(bytes, idx, p.LogStartOffset)
			idx = putInt(bytes, idx, -1)
			idx = putInt(bytes, idx, -1)
			idx = putRecordBatch(bytes, idx, p.RecordBatch.Bytes())
		}
	}
	return bytes
}

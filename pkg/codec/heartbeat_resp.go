package codec

type HeartBeatResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
}

func NewHeartBeatResp(version int16, corrId int) *HeartBeatResp {
	beatResp := HeartBeatResp{}
	beatResp.CorrelationId = corrId
	return &beatResp
}

func (h *HeartBeatResp) BytesLength() int {
	return LenCorrId + LenTaggedField + LenThrottleTime + LenErrorCode + LenTaggedField
}

func (h *HeartBeatResp) Bytes() []byte {
	bytes := make([]byte, h.BytesLength())
	idx := 0
	idx = putCorrId(bytes, idx, h.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, 0)
	idx = putErrorCode(bytes, idx, 0)
	idx = putTaggedField(bytes, idx)
	return bytes
}

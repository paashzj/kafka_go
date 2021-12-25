package codec

type HeartBeatResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
}

func NewHeartBeatResp(corrId int) *HeartBeatResp {
	beatResp := HeartBeatResp{}
	beatResp.CorrelationId = corrId
	return &beatResp
}

func (h *HeartBeatResp) BytesLength(version int16) int {
	return LenCorrId + LenTaggedField + LenThrottleTime + LenErrorCode + LenTaggedField
}

func (h *HeartBeatResp) Bytes(version int16) []byte {
	bytes := make([]byte, h.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, h.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, 0)
	idx = putErrorCode(bytes, idx, 0)
	idx = putTaggedField(bytes, idx)
	return bytes
}

package codec

type FindCoordinatorResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
	ErrorMessage *string
	NodeId       int
	Host         string
	Port         int
}

func NewFindCoordinatorResp(version int16, corrId int, config *KafkaProtocolConfig) *FindCoordinatorResp {
	findCoordinatorResp := FindCoordinatorResp{}
	findCoordinatorResp.CorrelationId = corrId
	findCoordinatorResp.NodeId = 0
	findCoordinatorResp.Host = config.AdvertiseHost
	findCoordinatorResp.Port = config.AdvertisePort
	return &findCoordinatorResp
}

func (f *FindCoordinatorResp) BytesLength() int {
	result := LenCorrId + LenTaggedField + LenThrottleTime
	result += LenErrorCode + CompactNullableStrLen(f.ErrorMessage) + LenNodeId
	return result + CompactStrLen(f.Host) + LenPort + LenTaggedField
}

func (f *FindCoordinatorResp) Bytes() []byte {
	bytes := make([]byte, f.BytesLength())
	idx := 0
	idx = putCorrId(bytes, idx, f.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, 0)
	idx = putErrorCode(bytes, idx, f.ErrorCode)
	idx = putFindCoordinatorErrorMessage(bytes, idx, f.ErrorMessage)
	idx = putInt(bytes, idx, f.NodeId)
	idx = putHost(bytes, idx, f.Host)
	idx = putInt(bytes, idx, f.Port)
	idx = putTaggedField(bytes, idx)
	return bytes
}

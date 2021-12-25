package codec

type SaslHandshakeResp struct {
	BaseResp
	ErrorCode        int16
	EnableMechanisms []*EnableMechanism
}

type EnableMechanism struct {
	SaslMechanism string
}

func NewSaslHandshakeResp(corrId int) *SaslHandshakeResp {
	saslHandshakeResp := SaslHandshakeResp{}
	saslHandshakeResp.CorrelationId = corrId
	return &saslHandshakeResp
}

func (s *SaslHandshakeResp) BytesLength(version int16) int {
	length := LenCorrId + LenErrorCode + LenArray
	for _, val := range s.EnableMechanisms {
		length += StrLen(val.SaslMechanism)
	}
	return length
}

// Bytes 转化为字节数组
func (s *SaslHandshakeResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putErrorCode(bytes, idx, s.ErrorCode)
	idx = putArrayLen(bytes, idx, len(s.EnableMechanisms))
	for _, val := range s.EnableMechanisms {
		idx = putSaslMechanism(bytes, idx, val.SaslMechanism)
	}
	return bytes
}

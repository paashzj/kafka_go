package codec

type SaslAuthenticateResp struct {
	BaseResp
	ErrorCode       int16
	ErrorMessage    string
	AuthBytes       []byte
	SessionLifetime int64
}

func NewSaslHandshakeAuthResp(version int16, corrId int) *SaslAuthenticateResp {
	handshakeAuthResp := SaslAuthenticateResp{}
	handshakeAuthResp.CorrelationId = corrId
	return &handshakeAuthResp
}

func (s *SaslAuthenticateResp) BytesLength(version int16) int {
	// 4字节CorrId + 1字节 tagged field + 2 字节ErrorCode + 变长ErrorMessage + 变长AuthBytes + 8 bytes ms + 1字节tagged field
	result := LenCorrId
	if version == 2 {
		result += LenTaggedField
	}
	result += LenErrorCode
	if version == 1 {
		result += StrLen(s.ErrorMessage)
	} else if version == 2 {
		result += CompactStrLen(s.ErrorMessage)
	}
	if version == 1 {
		result += BytesLen(s.AuthBytes)
	} else if version == 2 {
		result += CompactBytesLen(s.AuthBytes)
	}
	result += LenSessionTimeout
	if version == 2 {
		result += LenTaggedField
	}
	return result
}

// Bytes 转化为字节数组 tagged field 暂不实现
func (s *SaslAuthenticateResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	idx = putErrorCode(bytes, idx, s.ErrorCode)
	if version == 1 {
		idx = putErrorMessageString(bytes, idx, s.ErrorMessage)
	} else if version == 2 {
		idx = putErrorMessage(bytes, idx, s.ErrorMessage)
	}
	if version == 1 {
		idx = putSaslAuthBytes(bytes, idx, s.AuthBytes)
	} else if version == 2 {
		idx = putSaslAuthBytesCompact(bytes, idx, s.AuthBytes)
	}
	idx = putSessionLifeTimeout(bytes, idx, s.SessionLifetime)
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

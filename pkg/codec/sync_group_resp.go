package codec

type SyncGroupResp struct {
	BaseResp
	ThrottleTime     int
	ErrorCode        int16
	ProtocolType     string
	ProtocolName     string
	MemberAssignment string
}

func NewSyncGroupResp(corrId int) *SyncGroupResp {
	syncGroupResp := SyncGroupResp{}
	syncGroupResp.CorrelationId = corrId
	return &syncGroupResp
}

func (s *SyncGroupResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 4 || version == 5 {
		result += LenTaggedField + LenThrottleTime
	}
	result += LenErrorCode
	if version == 5 {
		result += CompactStrLen(s.ProtocolType) + CompactStrLen(s.ProtocolName)
	}
	if version == 0 {
		result += 2
		result += StrLen(s.MemberAssignment)
	} else if version == 4 || version == 5 {
		result += CompactStrLen(s.MemberAssignment)
	}
	if version == 4 || version == 5 {
		result += LenTaggedField
	}
	return result
}

func (s *SyncGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	if version == 4 || version == 5 {
		idx = putTaggedField(bytes, idx)
		idx = putUInt32(bytes, idx, 0)
	}
	idx = putErrorCode(bytes, idx, 0)
	if version == 5 {
		idx = putProtocolType(bytes, idx, s.ProtocolType)
		idx = putProtocolName(bytes, idx, s.ProtocolName)
	}
	if version == 0 {
		idx += 2
		idx = putString(bytes, idx, s.MemberAssignment)
	} else if version == 4 || version == 5 {
		idx = putCompactString(bytes, idx, s.MemberAssignment)
	}
	if version == 4 || version == 5 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

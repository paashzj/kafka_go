package codec

type SyncGroupResp struct {
	BaseResp
	ThrottleTime     int
	ErrorCode        int16
	ProtocolType     string
	ProtocolName     string
	MemberAssignment string
}

func NewSyncGroupResp(version int16, corrId int) *SyncGroupResp {
	syncGroupResp := SyncGroupResp{}
	syncGroupResp.CorrelationId = corrId
	return &syncGroupResp
}

func (s *SyncGroupResp) BytesLength(version int16) int {
	result := LenCorrId + LenTaggedField + LenThrottleTime + LenErrorCode
	if version == 5 {
		result += CompactStrLen(s.ProtocolType) + CompactStrLen(s.ProtocolName)
	}
	return result + CompactStrLen(s.MemberAssignment) + 1
}

func (s *SyncGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putUInt32(bytes, idx, 0)
	idx = putErrorCode(bytes, idx, 0)
	if version == 5 {
		idx = putProtocolType(bytes, idx, s.ProtocolType)
		idx = putProtocolName(bytes, idx, s.ProtocolName)
	}
	idx = putCompactString(bytes, idx, s.MemberAssignment)
	idx = putTaggedField(bytes, idx)
	return bytes
}

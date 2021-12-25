package codec

type LeaveGroupResp struct {
	BaseResp
	ErrorCode       int16
	ThrottleTime    int
	Members         []*LeaveGroupMember
	MemberErrorCode int16
}

func NewLeaveGroupResp(corrId int) *LeaveGroupResp {
	leaveGroupResp := LeaveGroupResp{}
	leaveGroupResp.CorrelationId = corrId
	return &leaveGroupResp
}

func (l *LeaveGroupResp) BytesLength(version int16) int {
	result := LenCorrId + LenTaggedField + LenThrottleTime + LenErrorCode + varintSize(len(l.Members)+1)
	for _, val := range l.Members {
		result += CompactStrLen(val.MemberId)
		result += CompactNullableStrLen(val.GroupInstanceId)
		result += LenTaggedField
	}
	return result + LenErrorCode + LenTaggedField
}

func (l *LeaveGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, l.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, l.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, l.ThrottleTime)
	idx = putErrorCode(bytes, idx, 0)
	bytes[idx] = byte(len(l.Members) + 1)
	idx++
	for _, member := range l.Members {
		idx = putMemberId(bytes, idx, member.MemberId)
		idx = putGroupInstanceId(bytes, idx, member.GroupInstanceId)
		idx = putTaggedField(bytes, idx)
	}
	idx = putErrorCode(bytes, idx, 0)
	idx = putTaggedField(bytes, idx)
	return bytes
}

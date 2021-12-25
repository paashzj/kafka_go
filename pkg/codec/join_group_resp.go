package codec

import "github.com/google/uuid"

type JoinGroupResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
	GenerationId int
	ProtocolType *string
	ProtocolName string
	LeaderId     string
	MemberId     string
	Members      []*Member
}

type Member struct {
	MemberId        string
	GroupInstanceId *string
	Metadata        string
}

func ErrorJoinGroupResp(corrId int, errorCode int16) *JoinGroupResp {
	joinGroupResp := JoinGroupResp{}
	joinGroupResp.CorrelationId = corrId
	joinGroupResp.ErrorCode = errorCode
	joinGroupResp.GenerationId = -1
	joinGroupResp.MemberId = uuid.New().String()
	return &joinGroupResp
}

func NewJoinGroupResp(version int16, corrId int) *JoinGroupResp {
	joinGroupResp := JoinGroupResp{}
	joinGroupResp.CorrelationId = corrId
	return &joinGroupResp
}

func (j *JoinGroupResp) BytesLength(version int16) int {
	result := LenCorrId + LenTaggedField + LenThrottleTime + LenErrorCode + LenGenerationId
	if version == 7 {
		result += CompactNullableStrLen(j.ProtocolType)
	}
	// 变长字符串protocolName + 变长字符串LeaderId + 1字节members长度
	// n个member长度（变长ConsumerGroupMemberId + 1字节ConsumerGroupInstance + 变长MemberMetadata + Tagged Fields）
	// TaggedFields
	result += CompactStrLen(j.ProtocolName)
	result += CompactStrLen(j.LeaderId)
	result += CompactStrLen(j.MemberId)
	result += varintSize(len(j.Members) + 1)
	for _, val := range j.Members {
		result += CompactStrLen(val.MemberId)
		result += CompactNullableStrLen(val.GroupInstanceId)
		result += CompactStrLen(val.Metadata)
		result += LenTaggedField
	}
	return result + 1
}

func (j *JoinGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, j.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, j.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, j.ThrottleTime)
	idx = putErrorCode(bytes, idx, j.ErrorCode)
	idx = putGenerationId(bytes, idx, j.GenerationId)
	if version == 7 {
		idx = putProtocolTypeNullable(bytes, idx, j.ProtocolType)
	}
	idx = putProtocolName(bytes, idx, j.ProtocolName)
	idx = putGroupLeaderId(bytes, idx, j.LeaderId)
	idx = putMemberId(bytes, idx, j.MemberId)
	idx = putCompactArrayLen(bytes, idx, len(j.Members))
	// put member
	for _, val := range j.Members {
		idx = putMemberId(bytes, idx, val.MemberId)
		idx = putGroupInstanceId(bytes, idx, val.GroupInstanceId)
		idx = putCompactString(bytes, idx, val.Metadata)
		idx = putTaggedField(bytes, idx)
	}
	idx = putTaggedField(bytes, idx)
	return bytes
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

func NewJoinGroupResp(corrId int) *JoinGroupResp {
	joinGroupResp := JoinGroupResp{}
	joinGroupResp.CorrelationId = corrId
	return &joinGroupResp
}

func (j *JoinGroupResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 6 || version == 7 {
		result += LenTaggedField + LenThrottleTime
	}
	result += LenErrorCode + LenGenerationId
	if version == 7 {
		result += CompactNullableStrLen(j.ProtocolType)
	}
	if version == 1 {
		result += StrLen(j.ProtocolName)
	} else if version == 6 || version == 7 {
		result += CompactStrLen(j.ProtocolName)
	}
	if version == 1 {
		result += StrLen(j.LeaderId)
	} else if version == 6 || version == 7 {
		result += CompactStrLen(j.LeaderId)
	}
	if version == 1 {
		result += StrLen(j.MemberId)
	} else if version == 6 || version == 7 {
		result += CompactStrLen(j.MemberId)
	}
	if version == 1 {
		result += LenArray
	} else if version == 6 || version == 7 {
		result += varintSize(len(j.Members) + 1)
	}
	for _, val := range j.Members {
		if version == 1 {
			result += StrLen(val.MemberId)
		} else if version == 6 || version == 7 {
			result += CompactStrLen(val.MemberId)
		}
		if version == 6 || version == 7 {
			result += CompactNullableStrLen(val.GroupInstanceId)
		}
		if version == 1 {
			result += 2 + StrLen(val.Metadata)
		} else if version == 6 || version == 7 {
			result += CompactStrLen(val.Metadata)
		}
		if version == 6 || version == 7 {
			result += LenTaggedField
		}
	}
	if version == 6 || version == 7 {
		result += LenTaggedField
	}
	return result
}

func (j *JoinGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, j.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, j.CorrelationId)
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, j.ThrottleTime)
	}
	idx = putErrorCode(bytes, idx, j.ErrorCode)
	idx = putGenerationId(bytes, idx, j.GenerationId)
	if version == 7 {
		idx = putProtocolTypeNullable(bytes, idx, j.ProtocolType)
	}
	if version == 1 {
		idx = putProtocolNameString(bytes, idx, j.ProtocolName)
	} else if version == 6 || version == 7 {
		idx = putProtocolName(bytes, idx, j.ProtocolName)
	}
	if version == 1 {
		idx = putGroupLeaderIdString(bytes, idx, j.LeaderId)
	} else if version == 6 || version == 7 {
		idx = putGroupLeaderId(bytes, idx, j.LeaderId)
	}
	if version == 1 {
		idx = putMemberIdString(bytes, idx, j.MemberId)
	} else if version == 6 || version == 7 {
		idx = putMemberId(bytes, idx, j.MemberId)
	}
	if version == 1 {
		idx = putArrayLen(bytes, idx, len(j.Members))
	} else if version == 6 || version == 7 {
		idx = putCompactArrayLen(bytes, idx, len(j.Members))
	}
	// put member
	for _, val := range j.Members {
		if version == 1 {
			idx = putMemberIdString(bytes, idx, val.MemberId)
		} else if version == 6 || version == 7 {
			idx = putMemberId(bytes, idx, val.MemberId)
		}
		if version == 6 || version == 7 {
			idx = putGroupInstanceId(bytes, idx, val.GroupInstanceId)
		}
		if version == 1 {
			idx = putBytes(bytes, idx, []byte(val.Metadata))
		} else if version == 6 || version == 7 {
			idx = putCompactString(bytes, idx, val.Metadata)
		}
		if version == 6 || version == 7 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

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

type OffsetCommitResp struct {
	BaseResp
	ThrottleTime  int
	TopicRespList []*OffsetCommitTopicResp
}

type OffsetCommitTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetCommitPartitionResp
}

type OffsetCommitPartitionResp struct {
	PartitionId int
	ErrorCode   int16
}

func NewOffsetCommitResp(corrId int) *OffsetCommitResp {
	CommitResp := OffsetCommitResp{}
	CommitResp.CorrelationId = corrId
	return &CommitResp
}

func (o *OffsetCommitResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 8 {
		result += LenTaggedField + LenThrottleTime
	}
	if version == 2 {
		result += LenArray
	} else if version == 8 {
		result += varintSize(len(o.TopicRespList) + 1)
	}
	for _, val := range o.TopicRespList {
		if version == 2 {
			result += StrLen(val.Topic)
		} else if version == 8 {
			result += CompactStrLen(val.Topic)
		}
		if version == 2 {
			result += LenArray
		} else if version == 8 {
			result += varintSize(len(val.PartitionRespList) + 1)
		}
		for range val.PartitionRespList {
			result += LenPartitionId + LenErrorCode
			if version == 8 {
				result += LenTaggedField
			}
		}
		if version == 8 {
			result += LenTaggedField
		}
	}
	if version == 8 {
		result += LenTaggedField
	}
	return result
}

func (o *OffsetCommitResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 8 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 2 {
		idx = putArrayLen(bytes, idx, len(o.TopicRespList))
	} else if version == 8 {
		idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	}
	for _, topic := range o.TopicRespList {
		if version == 2 {
			idx = putTopicString(bytes, idx, topic.Topic)
		} else if version == 8 {
			idx = putTopic(bytes, idx, topic.Topic)
		}
		if version == 2 {
			idx = putArrayLen(bytes, idx, len(topic.PartitionRespList))
		} else if version == 8 {
			idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		}
		for _, partition := range topic.PartitionRespList {
			idx = putInt(bytes, idx, partition.PartitionId)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			if version == 8 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 8 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 8 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

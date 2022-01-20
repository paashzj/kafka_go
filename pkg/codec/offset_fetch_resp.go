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

type OffsetFetchResp struct {
	BaseResp
	ThrottleTime  int
	ErrorCode     int16
	TopicRespList []*OffsetFetchTopicResp
}

type OffsetFetchTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetFetchPartitionResp
}

type OffsetFetchPartitionResp struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

func NewOffsetFetchResp(corrId int) *OffsetFetchResp {
	fetchResp := OffsetFetchResp{}
	fetchResp.CorrelationId = corrId
	return &fetchResp
}

func (o *OffsetFetchResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 6 {
		result += LenTaggedField + LenThrottleTime
	}
	if version == 1 {
		result += LenArray
	} else if version == 6 {
		result += varintSize(len(o.TopicRespList) + 1)
	}
	for _, val := range o.TopicRespList {
		if version == 1 {
			result += StrLen(val.Topic)
		} else if version == 6 {
			result += CompactStrLen(val.Topic)
		}
		if version == 1 {
			result += LenArray
		} else if version == 6 {
			result += varintSize(len(val.PartitionRespList) + 1)
		}
		for _, val2 := range val.PartitionRespList {
			result += LenPartitionId + LenOffset
			if version == 6 {
				result += LenLeaderEpoch
			}
			if version == 1 {
				result += StrLen(*val2.Metadata)
			} else if version == 6 {
				result += CompactNullableStrLen(val2.Metadata)
			}
			result += LenErrorCode
			if version == 6 {
				result += LenTaggedField
			}
		}
		if version == 6 {
			result += LenTaggedField
		}
	}
	if version == 6 {
		result += LenErrorCode + LenTaggedField
	}
	return result
}

func (o *OffsetFetchResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 6 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 1 {
		idx = putArrayLen(bytes, idx, len(o.TopicRespList))
	} else if version == 6 {
		idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	}
	for _, topic := range o.TopicRespList {
		if version == 1 {
			idx = putTopicString(bytes, idx, topic.Topic)
		} else if version == 6 {
			idx = putTopic(bytes, idx, topic.Topic)
		}
		if version == 1 {
			idx = putArrayLen(bytes, idx, len(topic.PartitionRespList))
		} else if version == 6 {
			idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		}
		for _, partition := range topic.PartitionRespList {
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putOffset(bytes, idx, partition.Offset)
			if version == 6 {
				idx = putLeaderEpoch(bytes, idx, partition.LeaderEpoch)
			}
			if version == 1 {
				idx = putString(bytes, idx, *partition.Metadata)
			} else if version == 6 {
				idx = putMetadata(bytes, idx, partition.Metadata)
			}
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			if version == 6 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 6 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 6 {
		idx = putErrorCode(bytes, idx, o.ErrorCode)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

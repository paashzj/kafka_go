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

type ListOffsetResp struct {
	BaseResp
	ErrorCode     int16
	ThrottleTime  int
	TopicRespList []*ListOffsetTopicResp
}

type ListOffsetTopicResp struct {
	Topic             string
	PartitionRespList []*ListOffsetPartitionResp
}

type ListOffsetPartitionResp struct {
	PartitionId int
	ErrorCode   int16
	Timestamp   int64
	Offset      int64
	LeaderEpoch int32
}

func NewListOffsetResp(corrId int) *ListOffsetResp {
	resp := ListOffsetResp{}
	resp.CorrelationId = corrId
	return &resp
}

func (o *ListOffsetResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 5 {
		result += LenThrottleTime
	}
	result += LenArray
	for _, val := range o.TopicRespList {
		result += StrLen(val.Topic) + LenArray
		for range val.PartitionRespList {
			result += LenPartitionId + LenErrorCode + LenTime + LenOffset
			if version == 5 {
				result += LenLeaderEpoch
			}
		}
	}
	return result
}

func (o *ListOffsetResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 5 {
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	idx = putArrayLen(bytes, idx, len(o.TopicRespList))
	for _, topic := range o.TopicRespList {
		idx = putTopicString(bytes, idx, topic.Topic)
		idx = putArrayLen(bytes, idx, len(topic.PartitionRespList))
		for _, p := range topic.PartitionRespList {
			idx = putInt(bytes, idx, p.PartitionId)
			idx = putErrorCode(bytes, idx, p.ErrorCode)
			idx = putInt64(bytes, idx, p.Timestamp)
			idx = putInt64(bytes, idx, p.Offset)
			if version == 5 {
				idx = putLeaderEpoch(bytes, idx, p.LeaderEpoch)
			}
		}
	}
	return bytes
}

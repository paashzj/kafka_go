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

type OffsetLeaderEpochResp struct {
	BaseResp
	ThrottleTime  int
	TopicRespList []*OffsetLeaderEpochTopicResp
}

type OffsetLeaderEpochTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetLeaderEpochPartitionResp
}

type OffsetLeaderEpochPartitionResp struct {
	ErrorCode   int16
	PartitionId int
	LeaderEpoch int32
	Offset      int64
}

func NewOffsetLeaderEpochResp(corrId int) *OffsetLeaderEpochResp {
	leaderEpochResp := OffsetLeaderEpochResp{}
	leaderEpochResp.CorrelationId = corrId
	return &leaderEpochResp
}

func (o *OffsetLeaderEpochResp) BytesLength(version int16) int {
	result := LenCorrId
	result += LenThrottleTime
	result += LenArray
	for _, val := range o.TopicRespList {
		result += StrLen(val.Topic)
		result += LenArray
		for range val.PartitionRespList {
			result += LenErrorCode
			result += LenPartitionId
			result += LenLeaderEpoch
			result += LenOffset
		}
	}
	return result
}

func (o *OffsetLeaderEpochResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	idx = putArrayLen(bytes, idx, len(o.TopicRespList))
	for _, topic := range o.TopicRespList {
		idx = putTopicString(bytes, idx, topic.Topic)
		idx = putArrayLen(bytes, idx, len(topic.PartitionRespList))
		for _, partition := range topic.PartitionRespList {
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putLeaderEpoch(bytes, idx, partition.LeaderEpoch)
			idx = putOffset(bytes, idx, partition.Offset)
		}
	}
	return bytes
}

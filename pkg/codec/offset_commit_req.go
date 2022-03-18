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

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type OffsetCommitReq struct {
	BaseReq
	GroupId                  string
	GenerationId             int
	MemberId                 string
	RetentionTime            int64
	GroupInstanceId          *string
	OffsetCommitTopicReqList []*OffsetCommitTopicReq
}

type OffsetCommitTopicReq struct {
	Topic            string
	OffsetPartitions []*OffsetCommitPartitionReq
}

type OffsetCommitPartitionReq struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int32
	Metadata    string
}

func DecodeOffsetCommitReq(bytes []byte, version int16) (offsetReq *OffsetCommitReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered in f", r, string(debug.Stack()))
			offsetReq = nil
			err = errors.New("codec failed")
		}
	}()
	offsetReq = &OffsetCommitReq{}
	idx := 0
	offsetReq.CorrelationId, idx = readCorrId(bytes, idx)
	offsetReq.ClientId, idx = readClientId(bytes, idx)
	if version == 8 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 2 {
		offsetReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 8 {
		offsetReq.GroupId, idx = readGroupId(bytes, idx)
	}
	offsetReq.GenerationId, idx = readGenerationId(bytes, idx)
	if version == 2 {
		offsetReq.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 8 {
		offsetReq.MemberId, idx = readMemberId(bytes, idx)
	}
	if version == 2 {
		offsetReq.RetentionTime, idx = readRetentionTime(bytes, idx)
	}
	if version == 8 {
		offsetReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	}
	var length int
	if version == 2 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 8 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	offsetReq.OffsetCommitTopicReqList = make([]*OffsetCommitTopicReq, length)
	for i := 0; i < length; i++ {
		topic := &OffsetCommitTopicReq{}
		if version == 2 {
			topic.Topic, idx = readTopicString(bytes, idx)
		} else if version == 8 {
			topic.Topic, idx = readTopic(bytes, idx)
		}
		var partitionLength int
		if version == 2 {
			partitionLength, idx = readArrayLen(bytes, idx)
		} else if version == 8 {
			partitionLength, idx = readCompactArrayLen(bytes, idx)
		}
		topic.OffsetPartitions = make([]*OffsetCommitPartitionReq, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &OffsetCommitPartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			partition.Offset, idx = readInt64(bytes, idx)
			if version == 8 {
				partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			if version == 2 {
				partition.Metadata, idx = readString(bytes, idx)
			} else if version == 8 {
				partition.Metadata, idx = readCompactString(bytes, idx)
			}
			if version == 8 {
				idx = readTaggedField(bytes, idx)
			}
			topic.OffsetPartitions[j] = partition
		}
		if version == 8 {
			idx = readTaggedField(bytes, idx)
		}
		offsetReq.OffsetCommitTopicReqList[i] = topic
	}
	if version == 8 {
		idx = readTaggedField(bytes, idx)
	}
	return offsetReq, nil
}

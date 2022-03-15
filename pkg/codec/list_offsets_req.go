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

type ListOffsetReq struct {
	BaseReq
	ReplicaId      int32
	IsolationLevel byte
	OffsetTopics   []*ListOffsetTopic
}

type ListOffsetTopic struct {
	Topic                string
	ListOffsetPartitions []*ListOffsetPartition
}

type ListOffsetPartition struct {
	PartitionId int
	LeaderEpoch int32
	Time        int64
}

func DecodeListOffsetReq(bytes []byte, version int16) (offsetReq *ListOffsetReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			offsetReq = nil
			err = errors.New("codec failed")
		}
	}()
	offsetReq = &ListOffsetReq{}
	idx := 0
	offsetReq.CorrelationId, idx = readCorrId(bytes, idx)
	offsetReq.ClientId, idx = readClientId(bytes, idx)
	offsetReq.ReplicaId, idx = readReplicaId(bytes, idx)
	if version == 5 {
		offsetReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	}
	var length int
	length, idx = readInt(bytes, idx)
	offsetReq.OffsetTopics = make([]*ListOffsetTopic, length)
	for i := 0; i < length; i++ {
		topic := &ListOffsetTopic{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx := readInt(bytes, idx)
		topic.ListOffsetPartitions = make([]*ListOffsetPartition, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &ListOffsetPartition{}
			partition.PartitionId, idx = readInt(bytes, idx)
			if version == 5 {
				partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			partition.Time, idx = readTime(bytes, idx)
			topic.ListOffsetPartitions[j] = partition
		}
		offsetReq.OffsetTopics[i] = topic
	}
	return offsetReq, nil
}

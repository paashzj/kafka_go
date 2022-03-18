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

type OffsetLeaderEpochReq struct {
	BaseReq
	ReplicaId    int32
	TopicReqList []*OffsetLeaderEpochTopicReq
}

type OffsetLeaderEpochTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetLeaderEpochPartitionReq
}

type OffsetLeaderEpochPartitionReq struct {
	PartitionId        int
	CurrentLeaderEpoch int32
	LeaderEpoch        int32
}

func DecodeOffsetLeaderEpochReq(bytes []byte, version int16) (leaderEpochReq *OffsetLeaderEpochReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			leaderEpochReq = nil
			err = errors.New("codec failed")
		}
	}()
	leaderEpochReq = &OffsetLeaderEpochReq{}
	idx := 0
	leaderEpochReq.CorrelationId, idx = readCorrId(bytes, idx)
	leaderEpochReq.ClientId, idx = readClientId(bytes, idx)
	leaderEpochReq.ReplicaId, idx = readReplicaId(bytes, idx)
	var length int
	length, idx = readArrayLen(bytes, idx)
	leaderEpochReq.TopicReqList = make([]*OffsetLeaderEpochTopicReq, length)
	for i := 0; i < length; i++ {
		topic := OffsetLeaderEpochTopicReq{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLen int
		partitionLen, idx = readArrayLen(bytes, idx)
		topic.PartitionReqList = make([]*OffsetLeaderEpochPartitionReq, partitionLen)
		for j := 0; j < partitionLen; j++ {
			o := &OffsetLeaderEpochPartitionReq{}
			o.PartitionId, idx = readPartitionId(bytes, idx)
			o.CurrentLeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			o.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			topic.PartitionReqList[j] = o
		}
		leaderEpochReq.TopicReqList[i] = &topic
	}
	return leaderEpochReq, nil
}

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

type FetchReq struct {
	BaseReq
	ReplicaId         int32
	MaxWaitTime       int
	MinBytes          int
	MaxBytes          int
	IsolationLevel    byte
	FetchSessionId    int
	FetchSessionEpoch int32
	TopicReqList      []*FetchTopicReq
}

type FetchTopicReq struct {
	Topic            string
	PartitionReqList []*FetchPartitionReq
}

type FetchPartitionReq struct {
	PartitionId        int
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int
	LogStartOffset     int64
	PartitionMaxBytes  int
}

func DecodeFetchReq(bytes []byte, version int16) (fetchReq *FetchReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered in f", r, string(debug.Stack()))
			fetchReq = nil
			err = errors.New("codec failed")
		}
	}()
	fetchReq = &FetchReq{}
	idx := 0
	fetchReq.CorrelationId, idx = readCorrId(bytes, idx)
	fetchReq.ClientId, idx = readClientId(bytes, idx)
	fetchReq.ReplicaId, idx = readReplicaId(bytes, idx)
	fetchReq.MaxWaitTime, idx = readInt(bytes, idx)
	fetchReq.MinBytes, idx = readInt(bytes, idx)
	fetchReq.MaxBytes, idx = readInt(bytes, idx)
	fetchReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	fetchReq.FetchSessionId, idx = readInt(bytes, idx)
	fetchReq.FetchSessionEpoch, idx = readFetchSessionEpoch(bytes, idx)
	var length int
	length, idx = readArrayLen(bytes, idx)
	fetchReq.TopicReqList = make([]*FetchTopicReq, length)
	for i := 0; i < length; i++ {
		topicReq := FetchTopicReq{}
		topicReq.Topic, idx = readTopicString(bytes, idx)
		var pLen int
		pLen, idx = readArrayLen(bytes, idx)
		topicReq.PartitionReqList = make([]*FetchPartitionReq, pLen)
		for j := 0; j < pLen; j++ {
			partition := &FetchPartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			partition.CurrentLeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			partition.FetchOffset, idx = readInt64(bytes, idx)
			partition.LogStartOffset, idx = readInt64(bytes, idx)
			partition.PartitionMaxBytes, idx = readInt(bytes, idx)
			topicReq.PartitionReqList[j] = partition
		}
		fetchReq.TopicReqList[i] = &topicReq
	}
	return fetchReq, nil
}

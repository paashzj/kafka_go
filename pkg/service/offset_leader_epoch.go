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

package service

import "net"

type OffsetLeaderEpochReq struct {
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

type OffsetLeaderEpochResp struct {
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

func OffsetLeaderEpoch(addr net.Addr, impl KfkServer, reqList []*OffsetLeaderEpochTopicReq) ([]*OffsetLeaderEpochTopicResp, error) {
	respList := make([]*OffsetLeaderEpochTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &OffsetLeaderEpochTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*OffsetLeaderEpochPartitionResp, 0)
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, _ := impl.OffsetLeaderEpoch(addr, topicReq.Topic, partitionReq)
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		respList[i] = f
	}
	return respList, nil
}

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

import (
	"net"
)

type OffsetFetchReq struct {
	GroupId      string
	TopicReqList []*OffsetFetchTopicReq
}

type OffsetFetchTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetFetchPartitionReq
}

type OffsetFetchPartitionReq struct {
	ClientId    string
	PartitionId int
	GroupId     string
}

type OffsetFetchResp struct {
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

func OffsetFetch(addr net.Addr, impl KfkServer, req *OffsetFetchReq) (*OffsetFetchResp, error) {
	reqList := req.TopicReqList
	result := &OffsetFetchResp{}
	result.TopicRespList = make([]*OffsetFetchTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &OffsetFetchTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*OffsetFetchPartitionResp, 0)
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, _ := impl.OffsetFetch(addr, topicReq.Topic, partitionReq)
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		result.TopicRespList[i] = f
	}
	return result, nil
}

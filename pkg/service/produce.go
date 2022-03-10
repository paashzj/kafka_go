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

type ProduceReq struct {
	GroupId      string
	TopicReqList []*ProduceTopicReq
}

type ProduceTopicReq struct {
	Topic            string
	PartitionReqList []*ProducePartitionReq
}

type ProducePartitionReq struct {
	ClientId    string
	PartitionId int
	RecordBatch *RecordBatch
}

type ProduceResp struct {
	ErrorCode     int16
	TopicRespList []*ProduceTopicResp
}

type ProduceTopicResp struct {
	Topic             string
	PartitionRespList []*ProducePartitionResp
}

type ProducePartitionResp struct {
	PartitionId    int
	ErrorCode      int16
	Offset         int64
	Time           int64
	LogStartOffset int64
}

func Produce(addr net.Addr, impl KfkServer, req *ProduceReq) (*ProduceResp, error) {
	reqList := req.TopicReqList
	result := &ProduceResp{}
	result.TopicRespList = make([]*ProduceTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &ProduceTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*ProducePartitionResp, 0)
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, _ := impl.Produce(addr, topicReq.Topic, partitionReq.PartitionId, partitionReq)
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		result.TopicRespList[i] = f
	}
	return result, nil
}

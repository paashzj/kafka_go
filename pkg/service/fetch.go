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

type FetchReq struct {
	MinBytes          int
	MaxBytes          int
	MaxWaitTime       int
	FetchTopicReqList []*FetchTopicReq
}

type FetchTopicReq struct {
	Topic                 string
	FetchPartitionReqList []*FetchPartitionReq
}

type FetchTopicResp struct {
	Topic                  string
	FetchPartitionRespList []*FetchPartitionResp
}

type FetchPartitionReq struct {
	PartitionId int
	FetchOffset int64
}

type FetchPartitionResp struct {
	PartitionId      int
	HighWatermark    int64
	LastStableOffset int64
	LogStartOffset   int64
	RecordBatch      *RecordBatch
}

func Fetch(addr *net.Addr, impl KfkServer, req *FetchReq) ([]*FetchTopicResp, error) {
	reqList := req.FetchTopicReqList
	result := make([]*FetchTopicResp, len(reqList))
	for i, req := range reqList {
		f := &FetchTopicResp{}
		f.Topic = req.Topic
		f.FetchPartitionRespList = make([]*FetchPartitionResp, len(req.FetchPartitionReqList))
		for j, partitionReq := range req.FetchPartitionReqList {
			// todo error 处理
			partition, _ := impl.FetchPartition(addr, req.Topic, partitionReq)
			f.FetchPartitionRespList[j] = partition
		}
		result[i] = f
	}
	return result, nil
}

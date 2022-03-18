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

type OffsetCommitTopicReq struct {
	Topic   string
	ReqList []*OffsetCommitPartitionReq
}

type OffsetCommitTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetCommitPartitionResp
}

type OffsetCommitPartitionReq struct {
	ClientId           string
	PartitionId        int
	OffsetCommitOffset int64
}

type OffsetCommitPartitionResp struct {
	PartitionId int
	ErrorCode   ErrorCode
}

func OffsetCommit(addr net.Addr, impl KfkServer, reqList []*OffsetCommitTopicReq) ([]*OffsetCommitTopicResp, error) {
	result := make([]*OffsetCommitTopicResp, len(reqList))
	for i, req := range reqList {
		f := &OffsetCommitTopicResp{}
		f.Topic = req.Topic
		f.PartitionRespList = make([]*OffsetCommitPartitionResp, len(req.ReqList))
		for j, partitionReq := range req.ReqList {
			// todo ignore error
			f.PartitionRespList[j], _ = impl.OffsetCommitPartition(addr, req.Topic, partitionReq)
		}
		result[i] = f
	}
	return result, nil
}

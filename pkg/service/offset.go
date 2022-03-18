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

type ListOffsetsTopicReq struct {
	Topic            string
	PartitionReqList []*ListOffsetsPartitionReq
}

type ListOffsetsTopicResp struct {
	Topic             string
	PartitionRespList []*ListOffsetsPartitionResp
}

type ListOffsetsPartitionReq struct {
	ClientId    string
	PartitionId int
	Time        int64
}

type ListOffsetsPartitionResp struct {
	ErrorCode   ErrorCode
	PartitionId int
	Time        int64
	Offset      int64
}

func Offset(addr net.Addr, impl KfkServer, reqList []*ListOffsetsTopicReq) ([]*ListOffsetsTopicResp, error) {
	result := make([]*ListOffsetsTopicResp, len(reqList))
	for i, req := range reqList {
		f := &ListOffsetsTopicResp{}
		f.Topic = req.Topic
		f.PartitionRespList = make([]*ListOffsetsPartitionResp, len(req.PartitionReqList))
		for j, partitionReq := range req.PartitionReqList {
			// todo ignore error
			f.PartitionRespList[j], _ = impl.OffsetListPartition(addr, f.Topic, partitionReq)
		}
		result[i] = f
	}
	return result, nil
}

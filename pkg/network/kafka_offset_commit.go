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

package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/paashzj/kafka_go/pkg/network/context"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) OffsetCommit(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 2 || version == 8 {
		return s.OffsetCommitVersion(ctx, frame, version)
	}
	logrus.Error("unknown fetch version ", version)
	return nil, gnet.Close
}

func (s *Server) OffsetCommitVersion(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	req, err := codec.DecodeOffsetCommitReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Debug("offset commit req ", req)
	lowReqList := make([]*service.OffsetCommitTopicReq, len(req.OffsetCommitTopicReqList))
	for i, topicReq := range req.OffsetCommitTopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, CONSUMER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
		lowTopicReq := &service.OffsetCommitTopicReq{}
		lowTopicReq.Topic = topicReq.Topic
		lowTopicReq.OffsetCommitPartitionReqList = make([]*service.OffsetCommitPartitionReq, len(topicReq.OffsetPartitions))
		for j, partitionReq := range topicReq.OffsetPartitions {
			lowPartitionReq := &service.OffsetCommitPartitionReq{}
			lowPartitionReq.PartitionId = partitionReq.PartitionId
			lowPartitionReq.OffsetCommitOffset = partitionReq.Offset
			lowPartitionReq.ClientId = req.ClientId
			lowTopicReq.OffsetCommitPartitionReqList[j] = lowPartitionReq
		}
		lowReqList[i] = lowTopicReq
	}
	lowTopicRespList, err := service.OffsetCommit(ctx.Addr, s.kafkaImpl, lowReqList)
	if err != nil {
		return nil, gnet.Close
	}
	resp := codec.NewOffsetCommitResp(req.CorrelationId)
	resp.Topics = make([]*codec.OffsetCommitTopicResp, len(lowTopicRespList))
	for i, lowTopicResp := range lowTopicRespList {
		f := &codec.OffsetCommitTopicResp{}
		f.Topic = lowTopicResp.Topic
		f.Partitions = make([]*codec.OffsetCommitPartitionResp, len(lowTopicResp.OffsetCommitPartitionRespList))
		for j, p := range lowTopicResp.OffsetCommitPartitionRespList {
			partitionResp := &codec.OffsetCommitPartitionResp{}
			partitionResp.PartitionId = p.PartitionId
			partitionResp.ErrorCode = int16(p.ErrorCode)
			f.Partitions[j] = partitionResp
		}
		resp.Topics[i] = f
	}
	return resp.Bytes(version), gnet.None
}

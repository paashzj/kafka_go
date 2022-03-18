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

func (s *Server) Produce(ctx *context.NetworkContext, frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 7 || version == 8 {
		return s.ReactProduceVersion(ctx, frame, version, config)
	}
	logrus.Error("unknown metadata version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactProduceVersion(ctx *context.NetworkContext, frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	req, err := codec.DecodeProduceReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Debug("produce req ", req)
	lowReq := &service.ProduceReq{}
	lowReq.TopicReqList = make([]*service.ProduceTopicReq, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, PRODUCER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
		lowTopicReq := &service.ProduceTopicReq{}
		lowTopicReq.Topic = topicReq.Topic
		lowTopicReq.PartitionReqList = make([]*service.ProducePartitionReq, len(topicReq.PartitionReqList))
		for j, partitionReq := range topicReq.PartitionReqList {
			lowPartitionReq := &service.ProducePartitionReq{}
			lowPartitionReq.PartitionId = partitionReq.PartitionId
			lowPartitionReq.ClientId = req.ClientId
			lowPartitionReq.RecordBatch = s.convertRecordBatchReq(partitionReq.RecordBatch)
			lowTopicReq.PartitionReqList[j] = lowPartitionReq
		}
		lowReq.TopicReqList[i] = lowTopicReq
	}
	lowResp, err := service.Produce(ctx.Addr, s.kafkaImpl, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	resp := codec.NewProduceResp(req.CorrelationId)
	resp.TopicRespList = make([]*codec.ProduceTopicResp, len(lowResp.TopicRespList))
	for i, lowTopicResp := range lowResp.TopicRespList {
		f := &codec.ProduceTopicResp{}
		f.Topic = lowTopicResp.Topic
		f.PartitionRespList = make([]*codec.ProducePartitionResp, len(lowTopicResp.PartitionRespList))
		for j, p := range lowTopicResp.PartitionRespList {
			partitionResp := &codec.ProducePartitionResp{}
			partitionResp.PartitionId = p.PartitionId
			partitionResp.ErrorCode = p.ErrorCode
			partitionResp.Offset = p.Offset
			partitionResp.Time = p.Time
			partitionResp.LogStartOffset = p.LogStartOffset
			f.PartitionRespList[j] = partitionResp
		}
		resp.TopicRespList[i] = f
	}
	return resp.Bytes(version), gnet.None
}

func (s *Server) convertRecordBatchReq(recordBatch *codec.RecordBatch) *service.RecordBatch {
	lowRecordBatch := &service.RecordBatch{}
	lowRecordBatch.Offset = recordBatch.Offset
	lowRecordBatch.LastOffsetDelta = recordBatch.LastOffsetDelta
	lowRecordBatch.FirstTimestamp = recordBatch.FirstTimestamp
	lowRecordBatch.LastTimestamp = recordBatch.LastTimestamp
	lowRecordBatch.BaseSequence = recordBatch.BaseSequence
	lowRecordBatch.Records = make([]*service.Record, len(recordBatch.Records))
	for i, r := range recordBatch.Records {
		record := &service.Record{}
		record.RelativeTimestamp = r.RelativeTimestamp
		record.RelativeOffset = r.RelativeOffset
		record.Key = r.Key
		record.Value = r.Value
		record.Headers = r.Headers
		lowRecordBatch.Records[i] = record
	}
	return lowRecordBatch
}

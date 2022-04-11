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

func (s *Server) Metadata(ctx *context.NetworkContext, frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 1 || version == 9 {
		return s.ReactMetadataVersion(ctx, frame, version, config)
	}
	logrus.Error("unknown metadata version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactMetadataVersion(ctx *context.NetworkContext, frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	metadataTopicReq, err := codec.DecodeMetadataTopicReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Debug("metadata req ", metadataTopicReq)
	topics := metadataTopicReq.Topics
	if len(topics) > 1 {
		logrus.Error("currently, not support more than one topic")
		return nil, gnet.Close
	}
	topic := topics[0].Topic
	var metadataResp *codec.MetadataResp
	partitionNum, err := s.kafkaImpl.PartitionNum(ctx.Addr, topic)
	if err != nil {
		metadataResp = codec.NewMetadataResp(metadataTopicReq.CorrelationId, config, topic, 0, int16(service.UNKNOWN_SERVER_ERROR))
	} else {
		metadataResp = codec.NewMetadataResp(metadataTopicReq.CorrelationId, config, topic, partitionNum, 0)
	}
	return metadataResp.Bytes(version), gnet.None
}

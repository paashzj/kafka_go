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
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) Metadata(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 1 || version == 9 {
		return s.ReactMetadataVersion(frame, version, config)
	}
	logrus.Error("unknown metadata version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactMetadataVersion(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	metadataTopicReq, err := codec.DecodeMetadataTopicReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Debug("metadata req ", metadataTopicReq)
	topics := metadataTopicReq.Topics
	metadataResp := codec.NewMetadataResp(metadataTopicReq.CorrelationId, config, topics[0].Topic, 0)
	return metadataResp.Bytes(version), gnet.None
}

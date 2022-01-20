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

func (s *Server) JoinGroup(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 1 || version == 6 || version == 7 {
		return s.ReactJoinGroupVersion(ctx, frame, version)
	}
	logrus.Error("unknown join group version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactJoinGroupVersion(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	req, err := codec.DecodeJoinGroupReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	if !s.checkSaslGroup(ctx, req.GroupId) {
		return nil, gnet.Close
	}
	logrus.Info("join group req", req)
	lowReq := &service.JoinGroupReq{}
	lowReq.GroupId = req.GroupId
	lowReq.SessionTimeout = req.SessionTimeout
	lowReq.MemberId = req.MemberId
	lowReq.GroupInstanceId = req.GroupInstanceId
	lowReq.ProtocolType = req.ProtocolType
	lowReq.GroupProtocols = make([]*service.GroupProtocol, len(req.GroupProtocols))
	for i, groupProtocol := range req.GroupProtocols {
		g := &service.GroupProtocol{}
		g.ProtocolName = groupProtocol.ProtocolName
		g.ProtocolMetadata = groupProtocol.ProtocolMetadata
		lowReq.GroupProtocols[i] = g
	}
	resp := codec.NewJoinGroupResp(req.CorrelationId)
	lowResp, err := s.kafkaImpl.GroupJoin(ctx.Addr, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Info("resp ", resp)
	resp.ErrorCode = int16(lowResp.ErrorCode)
	resp.GenerationId = lowResp.GenerationId
	resp.ProtocolType = lowResp.ProtocolType
	resp.ProtocolName = lowResp.ProtocolName
	resp.LeaderId = lowResp.LeaderId
	resp.MemberId = lowResp.MemberId
	resp.Members = make([]*codec.Member, len(lowResp.Members))
	for i, lowMember := range lowResp.Members {
		m := &codec.Member{}
		m.MemberId = lowMember.MemberId
		m.GroupInstanceId = lowMember.GroupInstanceId
		m.Metadata = lowMember.Metadata
		resp.Members[i] = m
	}
	return resp.Bytes(version), gnet.None
}

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

func (s *Server) LeaveGroup(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 0 || version == 4 {
		return s.ReactLeaveGroupVersion(ctx, frame, version)
	}
	logrus.Error("unknown leave group version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactLeaveGroupVersion(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	req, err := codec.DecodeLeaveGroupReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	if !s.checkSaslGroup(ctx, req.GroupId) {
		return nil, gnet.Close
	}
	logrus.Debug("leave group req ", req)
	lowReq := &service.LeaveGroupReq{}
	lowReq.GroupId = req.GroupId
	lowReq.ClientId = req.ClientId
	lowReq.Members = make([]*service.LeaveGroupMember, len(req.Members))
	for i, member := range req.Members {
		m := &service.LeaveGroupMember{}
		m.MemberId = member.MemberId
		m.GroupInstanceId = member.GroupInstanceId
		lowReq.Members[i] = m
	}
	resp := codec.NewLeaveGroupResp(req.CorrelationId)
	lowResp, err := s.kafkaImpl.GroupLeave(ctx.Addr, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	resp.ErrorCode = int16(lowResp.ErrorCode)
	resp.Members = make([]*codec.LeaveGroupMember, len(lowResp.Members))
	for i, member := range lowResp.Members {
		m := &codec.LeaveGroupMember{}
		m.MemberId = member.MemberId
		m.GroupInstanceId = member.GroupInstanceId
		resp.Members[i] = m
	}
	resp.MemberErrorCode = int16(lowResp.MemberErrorCode)
	return resp.Bytes(version), gnet.None
}

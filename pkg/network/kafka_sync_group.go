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

func (s *Server) SyncGroup(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 1 || version == 4 || version == 5 {
		return s.ReactSyncGroupVersion(ctx, frame, version)
	}
	logrus.Error("unknown sync group version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactSyncGroupVersion(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	req, err := codec.DecodeSyncGroupReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	if !s.checkSaslGroup(ctx, req.GroupId) {
		return nil, gnet.Close
	}
	logrus.Info("sync group req", req)
	lowReq := &service.SyncGroupReq{}
	lowReq.GroupId = req.GroupId
	lowReq.GenerationId = req.GenerationId
	lowReq.MemberId = req.MemberId
	lowReq.GroupInstanceId = req.GroupInstanceId
	lowReq.ProtocolType = req.ProtocolType
	lowReq.ProtocolName = req.ProtocolName
	lowReq.GroupAssignments = make([]*service.GroupAssignment, len(req.GroupAssignments))
	for i, groupAssignment := range req.GroupAssignments {
		g := &service.GroupAssignment{}
		g.MemberAssignment = groupAssignment.MemberAssignment
		g.MemberId = groupAssignment.MemberId
		lowReq.GroupAssignments[i] = g
	}
	resp := codec.NewSyncGroupResp(req.CorrelationId)
	lowResp, err := s.kafkaImpl.GroupSync(ctx.Addr, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	resp.ProtocolType = lowResp.ProtocolType
	resp.ProtocolName = lowResp.ProtocolName
	resp.MemberAssignment = lowResp.MemberAssignment
	return resp.Bytes(version), gnet.None
}

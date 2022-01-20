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

package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type SyncGroupReq struct {
	BaseReq
	GroupId          string
	GenerationId     int
	MemberId         string
	GroupInstanceId  *string
	ProtocolType     string
	ProtocolName     string
	GroupAssignments []*GroupAssignment
}

type GroupAssignment struct {
	MemberId string
	// COMPACT_BYTES
	MemberAssignment string
}

func DecodeSyncGroupReq(bytes []byte, version int16) (groupReq *SyncGroupReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			groupReq = nil
			err = errors.New("codec failed")
		}
	}()
	groupReq = &SyncGroupReq{}
	idx := 0
	groupReq.CorrelationId, idx = readCorrId(bytes, idx)
	groupReq.ClientId, idx = readClientId(bytes, idx)
	if version == 4 || version == 5 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		groupReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 4 || version == 5 {
		groupReq.GroupId, idx = readGroupId(bytes, idx)
	}
	groupReq.GenerationId, idx = readGenerationId(bytes, idx)
	if version == 0 {
		groupReq.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 4 || version == 5 {
		groupReq.MemberId, idx = readMemberId(bytes, idx)
	}
	if version == 4 || version == 5 {
		groupReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	}
	if version == 5 {
		groupReq.ProtocolType, idx = readProtocolType(bytes, idx)
		groupReq.ProtocolName, idx = readProtocolName(bytes, idx)
	}
	var groupAssignmentLength int
	if version == 0 {
		groupAssignmentLength, idx = readArrayLen(bytes, idx)
	} else if version == 4 || version == 5 {
		groupAssignmentLength, idx = readCompactArrayLen(bytes, idx)
	}
	groupReq.GroupAssignments = make([]*GroupAssignment, groupAssignmentLength)
	for i := 0; i < groupAssignmentLength; i++ {
		groupAssignment := GroupAssignment{}
		if version == 0 {
			groupAssignment.MemberId, idx = readMemberIdString(bytes, idx)
		} else if version == 4 || version == 5 {
			groupAssignment.MemberId, idx = readMemberId(bytes, idx)
		}
		if version == 0 {
			groupAssignment.MemberAssignment, idx = readString(bytes, idx)
		} else if version == 4 || version == 5 {
			groupAssignment.MemberAssignment, idx = readCompactString(bytes, idx)
		}
		if version == 4 || version == 5 {
			idx = readTaggedField(bytes, idx)
		}
		groupReq.GroupAssignments[i] = &groupAssignment
	}
	if version == 4 || version == 5 {
		idx = readTaggedField(bytes, idx)
	}
	return groupReq, nil
}

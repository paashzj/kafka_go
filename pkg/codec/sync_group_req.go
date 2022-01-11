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
	idx = readTaggedField(bytes, idx)
	groupReq.GroupId, idx = readGroupId(bytes, idx)
	groupReq.GenerationId, idx = readGenerationId(bytes, idx)
	groupReq.MemberId, idx = readMemberId(bytes, idx)
	groupReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	if version == 5 {
		groupReq.ProtocolType, idx = readProtocolType(bytes, idx)
		groupReq.ProtocolName, idx = readProtocolName(bytes, idx)
	}
	var groupAssignmentLength int
	groupAssignmentLength, idx = readCompactArrayLen(bytes, idx)
	groupReq.GroupAssignments = make([]*GroupAssignment, groupAssignmentLength)
	for i := 0; i < groupAssignmentLength; i++ {
		groupAssignment := GroupAssignment{}
		groupAssignment.MemberId, idx = readMemberId(bytes, idx)
		groupAssignment.MemberAssignment, idx = readCompactString(bytes, idx)
		idx = readTaggedField(bytes, idx)
		groupReq.GroupAssignments[i] = &groupAssignment
	}
	idx = readTaggedField(bytes, idx)
	return groupReq, nil
}

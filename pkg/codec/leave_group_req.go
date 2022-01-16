package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type LeaveGroupReq struct {
	BaseReq
	GroupId string
	Members []*LeaveGroupMember
}

func DecodeLeaveGroupReq(bytes []byte, version int16) (leaveGroupReq *LeaveGroupReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			leaveGroupReq = nil
			err = errors.New("codec failed")
		}
	}()
	leaveGroupReq = &LeaveGroupReq{}
	idx := 0
	leaveGroupReq.CorrelationId, idx = readCorrId(bytes, idx)
	leaveGroupReq.ClientId, idx = readClientId(bytes, idx)
	if version == 4 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		leaveGroupReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 4 {
		leaveGroupReq.GroupId, idx = readGroupId(bytes, idx)
	}
	if version == 0 {
		leaveGroupReq.Members = make([]*LeaveGroupMember, 1)
		member := LeaveGroupMember{}
		member.MemberId, idx = readMemberIdString(bytes, idx)
		leaveGroupReq.Members[0] = &member
	}
	if version == 4 {
		var length int
		length, idx = readCompactArrayLen(bytes, idx)
		leaveGroupReq.Members = make([]*LeaveGroupMember, length)
		for i := 0; i < length; i++ {
			member := LeaveGroupMember{}
			member.MemberId, idx = readMemberId(bytes, idx)
			member.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
			idx = readTaggedField(bytes, idx)
			leaveGroupReq.Members[i] = &member
		}
		idx = readTaggedField(bytes, idx)
	}
	return leaveGroupReq, nil
}

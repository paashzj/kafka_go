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
	idx = readTaggedField(bytes, idx)
	leaveGroupReq.GroupId, idx = readGroupId(bytes, idx)
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
	return leaveGroupReq, nil
}

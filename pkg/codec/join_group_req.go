package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type JoinGroupReq struct {
	BaseReq
	GroupId          string
	SessionTimeout   int
	RebalanceTimeout int
	MemberId         string
	GroupInstanceId  *string
	ProtocolType     string
	GroupProtocols   []*GroupProtocol
}

type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata string
}

func DecodeJoinGroupReq(bytes []byte, version int16) (joinGroupReq *JoinGroupReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			joinGroupReq = nil
			err = errors.New("codec failed")
		}
	}()
	joinGroupReq = &JoinGroupReq{}
	idx := 0
	joinGroupReq.CorrelationId, idx = readCorrId(bytes, idx)
	joinGroupReq.ClientId, idx = readClientId(bytes, idx)
	idx = readTaggedField(bytes, idx)
	joinGroupReq.GroupId, idx = readGroupId(bytes, idx)
	joinGroupReq.SessionTimeout, idx = readInt(bytes, idx)
	joinGroupReq.RebalanceTimeout, idx = readInt(bytes, idx)
	joinGroupReq.MemberId, idx = readMemberId(bytes, idx)
	joinGroupReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	joinGroupReq.ProtocolType, idx = readProtocolType(bytes, idx)
	var length int
	length, idx = readCompactArrayLen(bytes, idx)
	joinGroupReq.GroupProtocols = make([]*GroupProtocol, length)
	for i := 0; i < length; i++ {
		groupProtocol := GroupProtocol{}
		groupProtocol.ProtocolName, idx = readProtocolName(bytes, idx)
		groupProtocol.ProtocolMetadata, idx = readCompactString(bytes, idx)
		readTaggedField(bytes, idx)
		joinGroupReq.GroupProtocols[i] = &groupProtocol
	}
	readTaggedField(bytes, idx)
	return joinGroupReq, nil
}

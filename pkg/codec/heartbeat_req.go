package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type HeartBeatReq struct {
	BaseReq
	GroupId         string
	GenerationId    int
	MemberId        string
	GroupInstanceId *string
}

func DecodeHeartbeatReq(bytes []byte, version int16) (heartBeatReq *HeartBeatReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			heartBeatReq = nil
			err = errors.New("codec failed")
		}
	}()
	heartBeatReq = &HeartBeatReq{}
	idx := 0
	heartBeatReq.CorrelationId, idx = readCorrId(bytes, idx)
	heartBeatReq.ClientId, idx = readClientId(bytes, idx)
	idx = readTaggedField(bytes, idx)
	heartBeatReq.GroupId, idx = readGroupId(bytes, idx)
	heartBeatReq.GenerationId, idx = readGenerationId(bytes, idx)
	heartBeatReq.MemberId, idx = readMemberId(bytes, idx)
	heartBeatReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	idx = readTaggedField(bytes, idx)
	return heartBeatReq, nil
}

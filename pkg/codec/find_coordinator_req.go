package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type FindCoordinatorReq struct {
	BaseReq
	Key     string
	KeyType byte
}

func DecodeFindCoordinatorReq(bytes []byte, version int16) (findCoordinatorReq *FindCoordinatorReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			findCoordinatorReq = nil
			err = errors.New("codec failed")
		}
	}()
	findCoordinatorReq = &FindCoordinatorReq{}
	idx := 0
	findCoordinatorReq.CorrelationId, idx = readCorrId(bytes, idx)
	findCoordinatorReq.ClientId, idx = readClientId(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		findCoordinatorReq.Key, idx = readString(bytes, idx)
	} else if version == 3 {
		findCoordinatorReq.Key, idx = readCoordinatorKey(bytes, idx)
	}
	if version == 0 {
		findCoordinatorReq.KeyType = 0
	} else if version == 3 {
		findCoordinatorReq.KeyType, idx = readCoordinatorType(bytes, idx)
	}
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	return findCoordinatorReq, nil
}

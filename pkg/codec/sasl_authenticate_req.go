package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type SaslAuthenticateReq struct {
	BaseReq
	Username string
	Password string
}

func DecodeSaslHandshakeAuthReq(bytes []byte, version int16) (authReq *SaslAuthenticateReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			authReq = nil
			err = errors.New("codec failed")
		}
	}()
	authReq = &SaslAuthenticateReq{}
	idx := 0
	authReq.CorrelationId, idx = readCorrId(bytes, idx)
	authReq.ClientId, idx = readClientId(bytes, idx)
	var saslBytes []byte
	if version == 1 {
		saslBytes, idx = readBytes(bytes, idx)
	} else if version == 2 {
		idx = readTaggedField(bytes, idx)
		saslBytes, idx = readCompactBytes(bytes, idx)
	}
	authReq.Username, authReq.Password = readSaslAuthBytes(saslBytes, 0)
	if err != nil {
		return nil, errors.New("can't decode username or password")
	}
	if version == 2 {
		idx = readTaggedField(bytes, idx)
	}
	return authReq, nil
}

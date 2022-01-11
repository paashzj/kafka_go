package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type SaslHandshakeReq struct {
	BaseReq
	SaslMechanism string
}

// DecodeSaslHandshakeReq SaslHandshakeReq
func DecodeSaslHandshakeReq(bytes []byte, version int16) (saslHandshakeReq *SaslHandshakeReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			saslHandshakeReq = nil
			err = errors.New("codec failed")
		}
	}()
	req := &SaslHandshakeReq{}
	idx := 0
	req.CorrelationId, idx = readCorrId(bytes, idx)
	req.ClientId, idx = readClientId(bytes, idx)
	req.SaslMechanism, idx = readSaslMechanism(bytes, idx)
	return req, nil
}

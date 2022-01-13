package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type ApiReq struct {
	BaseReq
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

func DecodeApiReq(bytes []byte, version int16) (apiReq *ApiReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			apiReq = nil
			err = errors.New("codec failed")
		}
	}()
	apiReq = &ApiReq{}
	idx := 0
	apiReq.CorrelationId, idx = readCorrId(bytes, idx)
	apiReq.ClientId, idx = readClientId(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
		apiReq.ClientSoftwareName, idx = readClientSoftwareName(bytes, idx)
		apiReq.ClientSoftwareVersion, idx = readClientSoftwareVersion(bytes, idx)
		idx = readTaggedField(bytes, idx)
	}
	return apiReq, nil
}

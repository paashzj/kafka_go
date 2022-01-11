package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) ApiVersions(frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 0 || version == 3 {
		return s.ReactApiVersion(frame, version)
	}
	logrus.Error("unknown api version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactApiVersion(frame []byte, version int16) ([]byte, gnet.Action) {
	apiRequestV0, err := codec.DecodeApiReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Info("api request ", apiRequestV0)
	apiResponses := codec.NewApiVersionResp(apiRequestV0.CorrelationId)
	return apiResponses.Bytes(version), gnet.None
}

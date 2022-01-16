package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) FindCoordinator(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 0 || version == 3 {
		return s.FindCoordinatorVersion(frame, version, config)
	}
	logrus.Error("unknown find coordinator version ", version)
	return nil, gnet.Close
}

func (s *Server) FindCoordinatorVersion(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	req, err := codec.DecodeFindCoordinatorReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Info("req ", req)
	resp := codec.NewFindCoordinatorResp(req.CorrelationId, config)
	logrus.Info("resp ", resp)
	return resp.Bytes(version), gnet.None
}

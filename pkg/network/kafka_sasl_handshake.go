package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) SaslHandshake(frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 1 || version == 2 {
		return s.ReactSaslVersion(frame, version)
	}
	logrus.Error("unknown fetch version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactSaslVersion(frame []byte, version int16) ([]byte, gnet.Action) {
	req, err := codec.DecodeSaslHandshakeReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Info("sasl handshake request ", req)
	saslHandshakeResp := codec.NewSaslHandshakeResp(req.CorrelationId)
	saslHandshakeResp.EnableMechanisms = make([]*codec.EnableMechanism, 1)
	saslHandshakeResp.EnableMechanisms[0] = &codec.EnableMechanism{SaslMechanism: "PLAIN"}
	return saslHandshakeResp.Bytes(), gnet.None
}

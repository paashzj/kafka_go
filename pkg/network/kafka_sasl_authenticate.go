package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/paashzj/kafka_go/pkg/network/context"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) SaslAuthenticate(frame []byte, version int16, context *context.NetworkContext) ([]byte, gnet.Action) {
	if version == 1 || version == 2 {
		return s.ReactSaslHandshakeAuthVersion(frame, version, context)
	}
	logrus.Error("unknown handshake auth version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactSaslHandshakeAuthVersion(frame []byte, version int16, context *context.NetworkContext) ([]byte, gnet.Action) {
	req, err := codec.DecodeSaslHandshakeAuthReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Info("sasl handshake request ", req)
	saslHandshakeResp := codec.NewSaslHandshakeAuthResp(req.CorrelationId)
	saslReq := service.SaslReq{Username: req.Username, Password: req.Password}
	authResult, errorCode := service.SaslAuth(s.kafkaImpl, saslReq)
	if errorCode != 0 {
		return nil, gnet.Close
	}
	if authResult {
		context.Authed(true)
		s.SaslMap.Store(context.Addr, saslReq)
		return saslHandshakeResp.Bytes(version), gnet.None
	} else {
		return nil, gnet.Close
	}
}

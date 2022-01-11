package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) Metadata(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 9 {
		return s.ReactMetadataVersion(frame, version, config)
	}
	logrus.Error("unknown metadata version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactMetadataVersion(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	metadataTopicReq, err := codec.DecodeMetadataTopicReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	logrus.Info("metadata req ", metadataTopicReq)
	topics := metadataTopicReq.Topics
	metadataResp := codec.NewMetadataResp(metadataTopicReq.CorrelationId, config, topics[0].Topic, 0)
	return metadataResp.Bytes(), gnet.None
}

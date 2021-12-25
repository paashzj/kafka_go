package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"k8s.io/klog/v2"
)

func (s *Server) Metadata(frame []byte, version int16, config *codec.KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 1 || version == 9 {
		return s.ReactMetadataVersion(frame, version, config)
	}
	klog.Error("unknown metadata version ", version)
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
	return metadataResp.Bytes(version), gnet.None
}

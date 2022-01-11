package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type MetadataReq struct {
	BaseReq
	Topics                             []*MetadataTopicReq
	AllowAutoTopicCreation             bool
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
}

type MetadataTopicReq struct {
	Topic string
}

func DecodeMetadataTopicReq(bytes []byte, version int16) (metadataReq *MetadataReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			metadataReq = nil
			err = errors.New("codec failed")
		}
	}()
	metadataReq = &MetadataReq{}
	idx := 0
	metadataReq.CorrelationId, idx = readCorrId(bytes, idx)
	metadataReq.ClientId, idx = readClientId(bytes, idx)
	if version == 9 {
		idx = readTaggedField(bytes, idx)
	}
	length := int(bytes[idx] - 1)
	idx++
	metadataReq.Topics = make([]*MetadataTopicReq, length)
	for i := 0; i < length; i++ {
		metadataTopicReq := MetadataTopicReq{}
		metadataTopicReq.Topic, idx = readTopic(bytes, idx)
		readTaggedField(bytes, idx)
		metadataReq.Topics[i] = &metadataTopicReq
	}
	return metadataReq, nil
}

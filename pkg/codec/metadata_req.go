package codec

import (
	"errors"
	"k8s.io/klog/v2"
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
			klog.Info("Recovered in f", r, string(debug.Stack()))
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
	var length int
	if version == 1 {
		length, idx = readArrayLen(bytes, idx)
	} else {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	metadataReq.Topics = make([]*MetadataTopicReq, length)
	for i := 0; i < length; i++ {
		metadataTopicReq := MetadataTopicReq{}
		if version == 1 {
			metadataTopicReq.Topic, idx = readTopicString(bytes, idx)
		} else if version == 9 {
			metadataTopicReq.Topic, idx = readTopic(bytes, idx)
			readTaggedField(bytes, idx)
		}
		metadataReq.Topics[i] = &metadataTopicReq
	}
	return metadataReq, nil
}

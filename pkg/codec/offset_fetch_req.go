package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type OffsetFetchReq struct {
	BaseReq
	GroupId             string
	TopicReqList        []*OffsetFetchTopicReq
	RequireStableOffset bool
}

type OffsetFetchTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetFetchPartitionReq
}

type OffsetFetchPartitionReq struct {
	PartitionId int
}

func DecodeOffsetFetchReq(bytes []byte, version int16) (fetchReq *OffsetFetchReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			fetchReq = nil
			err = errors.New("codec failed")
		}
	}()
	fetchReq = &OffsetFetchReq{}
	idx := 0
	fetchReq.CorrelationId, idx = readCorrId(bytes, idx)
	fetchReq.ClientId, idx = readClientId(bytes, idx)
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 1 {
		fetchReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 6 || version == 7 {
		fetchReq.GroupId, idx = readGroupId(bytes, idx)
	}
	var length int
	if version == 1 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 6 || version == 7 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	fetchReq.TopicReqList = make([]*OffsetFetchTopicReq, length)
	for i := 0; i < length; i++ {
		topic := OffsetFetchTopicReq{}
		if version == 1 {
			topic.Topic, idx = readTopicString(bytes, idx)
		} else if version == 6 || version == 7 {
			topic.Topic, idx = readTopic(bytes, idx)
		}
		var partitionLen int
		if version == 1 {
			partitionLen, idx = readArrayLen(bytes, idx)
		} else if version == 6 || version == 7 {
			partitionLen, idx = readCompactArrayLen(bytes, idx)
		}
		topic.PartitionReqList = make([]*OffsetFetchPartitionReq, partitionLen)
		for j := 0; j < partitionLen; j++ {
			o := &OffsetFetchPartitionReq{}
			o.PartitionId, idx = readPartitionId(bytes, idx)
			topic.PartitionReqList[j] = o
		}
		if version == 6 || version == 7 {
			idx = readTaggedField(bytes, idx)
		}
		fetchReq.TopicReqList[i] = &topic
	}
	if version == 7 {
		if bytes[idx] == 1 {
			fetchReq.RequireStableOffset = true
		} else {
			fetchReq.RequireStableOffset = false
		}
		idx += 1
	}
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
	}
	return fetchReq, nil
}

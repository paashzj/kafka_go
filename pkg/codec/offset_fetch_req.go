package codec

import (
	"errors"
	"fmt"
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
	idx = readTaggedField(bytes, idx)
	fetchReq.GroupId, idx = readGroupId(bytes, idx)
	var length int
	length, idx = readCompactArrayLen(bytes, idx)
	fetchReq.TopicReqList = make([]*OffsetFetchTopicReq, length)
	for i := 0; i < length; i++ {
		topic := OffsetFetchTopicReq{}
		topic.Topic, idx = readTopic(bytes, idx)
		var partitionLen int
		partitionLen, idx = readCompactArrayLen(bytes, idx)
		topic.PartitionReqList = make([]*OffsetFetchPartitionReq, partitionLen)
		for j := 0; j < partitionLen; j++ {
			o := &OffsetFetchPartitionReq{}
			o.PartitionId, idx = readPartitionId(bytes, idx)
			fmt.Println(idx)
			topic.PartitionReqList[j] = o
		}
		idx = readTaggedField(bytes, idx)
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
	idx = readTaggedField(bytes, idx)
	return fetchReq, nil
}

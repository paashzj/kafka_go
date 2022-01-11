package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type OffsetCommitReq struct {
	BaseReq
	GroupId                  string
	GenerationId             int
	MemberId                 string
	GroupInstanceId          *string
	OffsetCommitTopicReqList []*OffsetCommitTopicReq
}

type OffsetCommitTopicReq struct {
	Topic            string
	OffsetPartitions []*OffsetCommitPartitionReq
}

type OffsetCommitPartitionReq struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int
	Metadata    string
}

func DecodeOffsetCommitReq(bytes []byte, version int16) (offsetReq *OffsetCommitReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			offsetReq = nil
			err = errors.New("codec failed")
		}
	}()
	offsetReq = &OffsetCommitReq{}
	idx := 0
	offsetReq.CorrelationId, idx = readCorrId(bytes, idx)
	offsetReq.ClientId, idx = readClientId(bytes, idx)
	idx = readTaggedField(bytes, idx)
	offsetReq.GroupId, idx = readGroupId(bytes, idx)
	offsetReq.GenerationId, idx = readInt(bytes, idx)
	offsetReq.MemberId, idx = readMemberId(bytes, idx)
	offsetReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	var length int
	length, idx = readCompactArrayLen(bytes, idx)
	offsetReq.OffsetCommitTopicReqList = make([]*OffsetCommitTopicReq, length)
	for i := 0; i < length; i++ {
		topic := &OffsetCommitTopicReq{}
		topic.Topic, idx = readTopic(bytes, idx)
		var partitionLength int
		partitionLength, idx = readCompactArrayLen(bytes, idx)
		topic.OffsetPartitions = make([]*OffsetCommitPartitionReq, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &OffsetCommitPartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			partition.Offset, idx = readInt64(bytes, idx)
			partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			partition.Metadata, idx = readCompactString(bytes, idx)
			idx = readTaggedField(bytes, idx)
			topic.OffsetPartitions[j] = partition
		}
		idx = readTaggedField(bytes, idx)
		offsetReq.OffsetCommitTopicReqList[i] = topic
	}
	idx = readTaggedField(bytes, idx)
	return offsetReq, nil
}

package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type ListOffsetReq struct {
	BaseReq
	ReplicaId      int32
	IsolationLevel byte
	OffsetTopics   []*ListOffsetTopic
}

type ListOffsetTopic struct {
	Topic                string
	ListOffsetPartitions []*ListOffsetPartition
}

type ListOffsetPartition struct {
	PartitionId int
	LeaderEpoch int32
	Time        int64
}

func DecodeListOffsetReq(bytes []byte, version int16) (offsetReq *ListOffsetReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			offsetReq = nil
			err = errors.New("codec failed")
		}
	}()
	offsetReq = &ListOffsetReq{}
	idx := 0
	offsetReq.CorrelationId, idx = readCorrId(bytes, idx)
	offsetReq.ClientId, idx = readClientId(bytes, idx)
	offsetReq.ReplicaId, idx = readReplicaId(bytes, idx)
	if version == 4 {
		offsetReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	}
	var length int
	length, idx = readInt(bytes, idx)
	offsetReq.OffsetTopics = make([]*ListOffsetTopic, length)
	for i := 0; i < length; i++ {
		topic := &ListOffsetTopic{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx := readInt(bytes, idx)
		topic.ListOffsetPartitions = make([]*ListOffsetPartition, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &ListOffsetPartition{}
			partition.PartitionId, idx = readInt(bytes, idx)
			if version == 4 {
				partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			partition.Time, idx = readTime(bytes, idx)
			topic.ListOffsetPartitions[j] = partition
		}
		offsetReq.OffsetTopics[i] = topic
	}
	return offsetReq, nil
}

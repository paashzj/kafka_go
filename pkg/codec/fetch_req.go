package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type FetchReq struct {
	BaseReq
	ReplicaId         int
	MaxWaitTime       int
	MinBytes          int
	MaxBytes          int
	IsolationLevel    byte
	FetchSessionId    int
	FetchSessionEpoch int
	FetchTopics       []*FetchTopicReq
}

type FetchTopicReq struct {
	Topic           string
	FetchPartitions []*FetchPartitionReq
}

type FetchPartitionReq struct {
	PartitionId        int
	CurrentLeaderEpoch int
	FetchOffset        int64
	LastFetchedEpoch   int
	LogStartOffset     int64
	PartitionMaxBytes  int
}

func DecodeFetchReq(bytes []byte, version int16) (fetchReq *FetchReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			fetchReq = nil
			err = errors.New("codec failed")
		}
	}()
	fetchReq = &FetchReq{}
	idx := 0
	fetchReq.CorrelationId, idx = readCorrId(bytes, idx)
	fetchReq.ClientId, idx = readClientId(bytes, idx)
	fetchReq.ReplicaId, idx = readInt(bytes, idx)
	fetchReq.MaxWaitTime, idx = readInt(bytes, idx)
	fetchReq.MinBytes, idx = readInt(bytes, idx)
	fetchReq.MaxBytes, idx = readInt(bytes, idx)
	fetchReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	fetchReq.FetchSessionId, idx = readInt(bytes, idx)
	fetchReq.FetchSessionEpoch, idx = readInt(bytes, idx)
	var length int
	length, idx = readArrayLen(bytes, idx)
	fetchReq.FetchTopics = make([]*FetchTopicReq, length)
	for i := 0; i < length; i++ {
		topicReq := FetchTopicReq{}
		topicReq.Topic, idx = readTopicString(bytes, idx)
		var pLen int
		pLen, idx = readArrayLen(bytes, idx)
		topicReq.FetchPartitions = make([]*FetchPartitionReq, pLen)
		for j := 0; j < pLen; j++ {
			partition := &FetchPartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			partition.CurrentLeaderEpoch, idx = readInt(bytes, idx)
			partition.FetchOffset, idx = readInt64(bytes, idx)
			partition.LogStartOffset, idx = readInt64(bytes, idx)
			partition.PartitionMaxBytes, idx = readInt(bytes, idx)
			topicReq.FetchPartitions[i] = partition
		}
		fetchReq.FetchTopics[i] = &topicReq
	}
	return fetchReq, nil
}

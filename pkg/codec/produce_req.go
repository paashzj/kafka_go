package codec

import (
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type ProduceReq struct {
	BaseReq
	ClientId      string
	TransactionId int16
	RequiredAcks  int16
	Timeout       int
	TopicReqList  []*ProduceTopicReq
}

type ProduceTopicReq struct {
	Topic            string
	PartitionReqList []*ProducePartitionReq
}

type ProducePartitionReq struct {
	PartitionId int
	RecordBatch *RecordBatch
}

func DecodeProduceReq(bytes []byte, version int16) (produceReq *ProduceReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			produceReq = nil
			err = errors.New("codec failed")
		}
	}()
	produceReq = &ProduceReq{}
	idx := 0
	produceReq.CorrelationId, idx = readCorrId(bytes, idx)
	produceReq.ClientId, idx = readClientId(bytes, idx)
	// todo skip transactionId
	idx += 2
	// todo skip requiredAcks
	idx += 2
	produceReq.Timeout, idx = readInt(bytes, idx)
	var length int
	length, idx = readInt(bytes, idx)
	produceReq.TopicReqList = make([]*ProduceTopicReq, length)
	for i := 0; i < length; i++ {
		topic := &ProduceTopicReq{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx = readInt(bytes, idx)
		topic.PartitionReqList = make([]*ProducePartitionReq, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &ProducePartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			recordBatchLength, idx := readInt(bytes, idx)
			partition.RecordBatch = DecodeRecordBatch(bytes[idx:idx+recordBatchLength-1], version)
			idx += recordBatchLength
			topic.PartitionReqList[i] = partition
		}
		produceReq.TopicReqList[i] = topic
	}
	return produceReq, nil
}

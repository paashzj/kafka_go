package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeProduceReqV7(t *testing.T) {
	bytes := testHex2Bytes(t, "00000002006d5f5f5f546573744b61666b6150726f647563655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff00000f9a000000010005746f70696300000001000000000000004700000000000000000000003bffffffff022c30096c0000000000000000017df19951180000017df1995118ffffffffffffffffffffffffffff000000011200000001066d736700")
	produceReq, err := DecodeProduceReq(bytes, 7)
	assert.Nil(t, err)
	assert.Equal(t, 2, produceReq.CorrelationId)
	assert.Equal(t, "___TestKafkaProduce_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", produceReq.ClientId)
	assert.Equal(t, 3994, produceReq.Timeout)
	assert.Len(t, produceReq.TopicReqList, 1)
	topicReq := produceReq.TopicReqList[0]
	assert.Equal(t, "topic", topicReq.Topic)
	assert.Len(t, topicReq.PartitionReqList, 1)
	partitionReq := topicReq.PartitionReqList[0]
	assert.Equal(t, 0, partitionReq.PartitionId)
	recordBatch := partitionReq.RecordBatch
	var expectedOffset int64 = 0
	assert.Equal(t, expectedOffset, recordBatch.Offset)
	assert.Equal(t, 59, recordBatch.MessageSize)
	var expectedLeaderEpoch int32 = -1
	assert.Equal(t, expectedLeaderEpoch, recordBatch.LeaderEpoch)
	var expectedMagicByte byte = 2
	assert.Equal(t, expectedMagicByte, recordBatch.MagicByte)
	var expectedFlags uint16 = 0
	assert.Equal(t, expectedFlags, recordBatch.Flags)
	assert.Equal(t, 0, recordBatch.LastOffsetDelta)
	var expectedProducerId int64 = -1
	assert.Equal(t, expectedProducerId, recordBatch.ProducerId)
	var expectedBaseSequence int32 = -1
	assert.Equal(t, expectedBaseSequence, recordBatch.BaseSequence)
	assert.Len(t, recordBatch.Records, 1)
	record := recordBatch.Records[0]
	var expectedRecordAttributes byte = 0
	assert.Equal(t, expectedRecordAttributes, record.RecordAttributes)
	var expectedRelativeTimestamp int64 = 0
	assert.Equal(t, expectedRelativeTimestamp, record.RelativeTimestamp)
	assert.Equal(t, 0, record.RelativeOffset)
	assert.Nil(t, record.Key)
	assert.Equal(t, "msg", record.Value)
}

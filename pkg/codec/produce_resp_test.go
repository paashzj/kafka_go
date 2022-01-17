package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeProduceRespV7(t *testing.T) {
	produceResp := NewProduceResp(2)
	producePartitionResp := &ProducePartitionResp{}
	producePartitionResp.PartitionId = 0
	producePartitionResp.ErrorCode = 0
	producePartitionResp.Offset = 0
	producePartitionResp.Time = -1
	producePartitionResp.LogStartOffset = 0
	produceTopicResp := &ProduceTopicResp{}
	produceTopicResp.Topic = "topic"
	produceTopicResp.PartitionRespList = []*ProducePartitionResp{producePartitionResp}
	produceResp.TopicRespList = []*ProduceTopicResp{produceTopicResp}
	bytes := produceResp.Bytes(7)
	expectBytes := testHex2Bytes(t, "00000002000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeProduceRespV8(t *testing.T) {
	produceResp := NewProduceResp(4)
	producePartitionResp := &ProducePartitionResp{}
	producePartitionResp.PartitionId = 0
	producePartitionResp.ErrorCode = 0
	producePartitionResp.Offset = 0
	producePartitionResp.Time = -1
	producePartitionResp.LogStartOffset = 0
	produceTopicResp := &ProduceTopicResp{}
	produceTopicResp.Topic = "topic"
	produceTopicResp.PartitionRespList = []*ProducePartitionResp{producePartitionResp}
	produceResp.TopicRespList = []*ProduceTopicResp{produceTopicResp}
	bytes := produceResp.Bytes(8)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000ffff00000000")
	assert.Equal(t, expectBytes, bytes)
}

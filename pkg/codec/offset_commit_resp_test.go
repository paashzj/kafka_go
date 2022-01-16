package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeOffsetCommitRespV2(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "topic"
	offsetCommitTopicResp.Partitions = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := NewOffsetCommitResp(5)
	offsetCommitResp.Topics = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(2)
	expectBytes := testHex2Bytes(t, "00000005000000010005746f70696300000001000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeOffsetCommitRespV8(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "test-5"
	offsetCommitTopicResp.Partitions = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := NewOffsetCommitResp(11)
	offsetCommitResp.Topics = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(8)
	expectBytes := testHex2Bytes(t, "0000000b00000000000207746573742d3502000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeOffsetCommitRespV8(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "test-5"
	offsetCommitTopicResp.Partitions = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := NewOffsetCommitResp(0, 11)
	offsetCommitResp.Topics = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(5)
	expectBytes := testHex2Bytes(t, "0000000b00000000000207746573742d3502000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

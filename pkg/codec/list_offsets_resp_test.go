package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeListOffsetsRespV5(t *testing.T) {
	listOffsetPartitionResp := &ListOffsetPartitionResp{}
	listOffsetPartitionResp.PartitionId = 0
	listOffsetPartitionResp.ErrorCode = 0
	listOffsetPartitionResp.Timestamp = -1
	listOffsetPartitionResp.Offset = 0
	listOffsetPartitionResp.LeaderEpoch = 0
	listOffsetTopicResp := &ListOffsetTopicResp{}
	listOffsetTopicResp.Topic = "test-5"
	listOffsetTopicResp.ListOffsetPartitions = []*ListOffsetPartitionResp{listOffsetPartitionResp}
	listOffsetResp := NewListOffsetResp(0, 8)
	listOffsetResp.OffsetTopics = []*ListOffsetTopicResp{listOffsetTopicResp}
	bytes := listOffsetResp.Bytes(5)
	expectBytes := testHex2Bytes(t, "0000000800000000000000010006746573742d3500000001000000000000ffffffffffffffff000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

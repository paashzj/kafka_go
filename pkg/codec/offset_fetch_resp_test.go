package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeOffsetFetchRespV6(t *testing.T) {
	offsetFetchResp := NewOffsetFetchResp(0, 7)
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	offsetFetchPartitionResp.LeaderEpoch = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "test-5"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(6)
	expectBytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	assert.Equal(t, expectBytes, bytes)
}

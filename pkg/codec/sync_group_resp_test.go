package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeSyncGroupRespV4(t *testing.T) {
	syncGroupResp := NewSyncGroupResp(0, 6)
	syncGroupResp.ProtocolType = ""
	syncGroupResp.ProtocolName = ""
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "00000006000000000000001b0001000000010006746573742d350000000100000000ffffffff00")
	assert.Equal(t, expectBytes, bytes)
}

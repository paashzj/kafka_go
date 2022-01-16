package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeSyncGroupRespV0(t *testing.T) {
	syncGroupResp := NewSyncGroupResp(3)
	syncGroupResp.ErrorCode = 0
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010005746f7069630000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "000000030000000000190001000000010005746f7069630000000100000000ffffffff")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeSyncGroupRespV4(t *testing.T) {
	syncGroupResp := NewSyncGroupResp(6)
	syncGroupResp.ProtocolType = ""
	syncGroupResp.ProtocolName = ""
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "00000006000000000000001b0001000000010006746573742d350000000100000000ffffffff00")
	assert.Equal(t, expectBytes, bytes)
}

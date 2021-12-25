package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeHeartbeatRespV4(t *testing.T) {
	heartBeatResp := NewHeartBeatResp(0, 17)
	bytes := heartBeatResp.Bytes()
	expectBytes := testHex2Bytes(t, "000000110000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

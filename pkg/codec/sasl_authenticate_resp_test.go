package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeSaslHandshakeAuthRespV2(t *testing.T) {
	saslHandshakeAuthResp := NewSaslHandshakeAuthResp(0, 2147483642)
	bytes := saslHandshakeAuthResp.Bytes(2)
	expectBytes := testHex2Bytes(t, "7ffffffa0000000101000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeSaslHandshakeRespV1(t *testing.T) {
	saslHandshakeResp := NewSaslHandshakeResp(2147483641)
	plainSaslMechanism := &EnableMechanism{SaslMechanism: "PLAIN"}
	saslHandshakeResp.EnableMechanisms = []*EnableMechanism{plainSaslMechanism}
	bytes := saslHandshakeResp.Bytes(1)
	expectBytes := testHex2Bytes(t, "7ffffff90000000000010005504c41494e")
	assert.Equal(t, expectBytes, bytes)
}

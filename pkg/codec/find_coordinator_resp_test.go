package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeFindCoordinatorRespV3(t *testing.T) {
	protocolConfig := KafkaProtocolConfig{}
	protocolConfig.ClusterId = "shoothzj"
	protocolConfig.AdvertiseHost = "localhost"
	protocolConfig.AdvertisePort = 9092
	findCoordinatorResp := NewFindCoordinatorResp(0, 0, &protocolConfig)
	bytes := findCoordinatorResp.Bytes()
	expectBytes := testHex2Bytes(t, "000000000000000000000000000000000a6c6f63616c686f73740000238400")
	assert.Equal(t, expectBytes, bytes)
}

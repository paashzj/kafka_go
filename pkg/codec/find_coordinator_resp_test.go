package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeFindCoordinatorRespV0(t *testing.T) {
	protocolConfig := KafkaProtocolConfig{}
	protocolConfig.AdvertiseHost = "localhost"
	protocolConfig.AdvertisePort = 9092
	protocolConfig.NodeId = 1
	findCoordinatorResp := NewFindCoordinatorResp(1, &protocolConfig)
	bytes := findCoordinatorResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "0000000100000000000100096c6f63616c686f737400002384")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeFindCoordinatorRespV3(t *testing.T) {
	protocolConfig := KafkaProtocolConfig{}
	protocolConfig.ClusterId = "shoothzj"
	protocolConfig.AdvertiseHost = "localhost"
	protocolConfig.AdvertisePort = 9092
	findCoordinatorResp := NewFindCoordinatorResp(0, &protocolConfig)
	bytes := findCoordinatorResp.Bytes(3)
	expectBytes := testHex2Bytes(t, "000000000000000000000000000000000a6c6f63616c686f73740000238400")
	assert.Equal(t, expectBytes, bytes)
}

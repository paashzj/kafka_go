package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeIllegalApiVersionReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeApiReq(bytes, 1)
	assert.NotNil(t, err)
}

func TestDecodeApiVersionReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001006d5f5f5f546573744b61666b6150726f647563655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29")
	apiReq, err := DecodeApiReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, apiReq.CorrelationId)
	assert.Equal(t, "___TestKafkaProduce_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", apiReq.ClientId)
}

func TestDecodeApiVersionReqV3(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d3100126170616368652d6b61666b612d6a61766106322e342e3000")
	apiReq, err := DecodeApiReq(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, 1, apiReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", apiReq.ClientId)
	assert.Equal(t, "apache-kafka-java", apiReq.ClientSoftwareName)
	assert.Equal(t, "2.4.0", apiReq.ClientSoftwareVersion)
}

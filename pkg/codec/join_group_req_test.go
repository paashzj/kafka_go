package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeIllegalJoinGroupReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeJoinGroupReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeJoinGroupReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f290005746f706963000075300000753000000008636f6e73756d657200000002000572616e6765000000110001000000010005746f706963ffffffff000a726f756e64726f62696e000000110001000000010005746f706963ffffffff")
	joinGroupReq, err := DecodeJoinGroupReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, joinGroupReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", joinGroupReq.ClientId)
	assert.Equal(t, "topic", joinGroupReq.GroupId)
	assert.Equal(t, 30_000, joinGroupReq.SessionTimeout)
	assert.Equal(t, 30_000, joinGroupReq.RebalanceTimeout)
	assert.Equal(t, "", joinGroupReq.MemberId)
	assert.Equal(t, "consumer", joinGroupReq.ProtocolType)
	assert.Len(t, joinGroupReq.GroupProtocols, 2)
	groupProtocol1 := joinGroupReq.GroupProtocols[0]
	assert.Equal(t, "range", groupProtocol1.ProtocolName)
	groupProtocol2 := joinGroupReq.GroupProtocols[1]
	assert.NotNil(t, groupProtocol2)
}

func TestDecodeJoinGroupReqV6(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d31002537336664633964612d306439322d346537622d613761372d65633236366636376331373100002710000493e0010009636f6e73756d6572020672616e676535000100000001002437363465646565332d303037652d343865302d623966392d646637663731336666373037ffffffff000000000000")
	joinGroupReq, err := DecodeJoinGroupReq(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, 8, joinGroupReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", joinGroupReq.ClientId)
	assert.Equal(t, "73fdc9da-0d92-4e7b-a7a7-ec266f67c171", joinGroupReq.GroupId)
	assert.Equal(t, 10_000, joinGroupReq.SessionTimeout)
	assert.Equal(t, 300_000, joinGroupReq.RebalanceTimeout)
	assert.Equal(t, "", joinGroupReq.MemberId)
	assert.Equal(t, "consumer", joinGroupReq.ProtocolType)
	assert.Len(t, joinGroupReq.GroupProtocols, 1)
	groupProtocol := joinGroupReq.GroupProtocols[0]
	assert.Equal(t, "range", groupProtocol.ProtocolName)
}

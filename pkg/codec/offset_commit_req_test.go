// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeIllegalOffsetCommitReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeOffsetCommitReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeOffsetCommitReqV2(t *testing.T) {
	bytes := testHex2Bytes(t, "00000005006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f290005746f7069630000000300925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d326132666565363938396338ffffffffffffffff000000010005746f706963000000010000000000000000000000010000")
	offsetCommitReq, err := DecodeOffsetCommitReq(bytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, 5, offsetCommitReq.CorrelationId)
	assert.Equal(t, 3, offsetCommitReq.GenerationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", offsetCommitReq.ClientId)
	assert.Equal(t, "topic", offsetCommitReq.GroupId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8", offsetCommitReq.MemberId)
	assert.Len(t, offsetCommitReq.TopicReqList, 1)
	offsetTopic := offsetCommitReq.TopicReqList[0]
	assert.Equal(t, "topic", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	var expectedOffset int64 = 1
	assert.Equal(t, expectedOffset, offsetPartition.Offset)
}

func TestDecodeOffsetCommitReqV8(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000b002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31002538646437623936622d366239342d346139622d623263632d3363623538393863396364660000000155636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d366132343963666630376663000207746573742d35020000000000000000000000010000000001000000")
	offsetCommitReq, err := DecodeOffsetCommitReq(bytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, 11, offsetCommitReq.CorrelationId)
	assert.Equal(t, 1, offsetCommitReq.GenerationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", offsetCommitReq.ClientId)
	assert.Equal(t, "8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf", offsetCommitReq.GroupId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", offsetCommitReq.MemberId)
	assert.Len(t, offsetCommitReq.TopicReqList, 1)
	offsetTopic := offsetCommitReq.TopicReqList[0]
	assert.Equal(t, "test-5", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	var expectedOffset int64 = 1
	assert.Equal(t, expectedOffset, offsetPartition.Offset)
}

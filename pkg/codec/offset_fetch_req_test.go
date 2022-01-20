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

func TestDecodeIllegalOffsetFetchReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeOffsetFetchReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeOffsetFetchReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f290005746f706963000000010005746f7069630000000100000000")
	fetchReq, err := DecodeOffsetFetchReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, fetchReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", fetchReq.ClientId)
	assert.Equal(t, "topic", fetchReq.GroupId)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "topic", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	assert.False(t, fetchReq.RequireStableOffset)
}

func TestDecodeOffsetFetchReqV6(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000b002f636f6e73756d65722d61303332616233632d303831382d343937352d626439332d3735613431323030656162342d31002561303332616233632d303831382d343937352d626439332d373561343132303065616234020a746573742d7361736c02000000000000")
	fetchReq, err := DecodeOffsetFetchReq(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, 11, fetchReq.CorrelationId)
	assert.Equal(t, "consumer-a032ab3c-0818-4975-bd93-75a41200eab4-1", fetchReq.ClientId)
	assert.Equal(t, "a032ab3c-0818-4975-bd93-75a41200eab4", fetchReq.GroupId)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test-sasl", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	assert.False(t, fetchReq.RequireStableOffset)
}

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

func TestDecodeIllegalListOffsetReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeListOffsetReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeListOffsetsReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000000010005746f7069630000000100000000ffffffffffffffff")
	listOffsetReq, err := DecodeListOffsetReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, listOffsetReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", listOffsetReq.ClientId)
	var expectedReplicaId int32 = -1
	assert.Equal(t, expectedReplicaId, listOffsetReq.ReplicaId)
	var expectedIsolationLevel uint8 = 0
	assert.Equal(t, expectedIsolationLevel, listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "topic", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	var expectedLeaderEpoch int32 = 0
	assert.Equal(t, expectedLeaderEpoch, offsetPartition.LeaderEpoch)
	var expectedPartitionTime int64 = -1
	assert.Equal(t, expectedPartitionTime, offsetPartition.Time)
}

func TestDecodeListOffsetsReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff00000000010006746573742d35000000010000000000000000fffffffffffffffe")
	listOffsetReq, err := DecodeListOffsetReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, 8, listOffsetReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", listOffsetReq.ClientId)
	var expectedReplicaId int32 = -1
	assert.Equal(t, expectedReplicaId, listOffsetReq.ReplicaId)
	var expectedIsolationLevel uint8 = 0
	assert.Equal(t, expectedIsolationLevel, listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "test-5", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	var expectedLeaderEpoch int32 = 0
	assert.Equal(t, expectedLeaderEpoch, offsetPartition.LeaderEpoch)
	var expectedPartitionTime int64 = -2
	assert.Equal(t, expectedPartitionTime, offsetPartition.Time)
}

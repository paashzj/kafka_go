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

func TestDecodeIllegalFindCoordinatorReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeFindCoordinatorReq(bytes, 1)
	assert.NotNil(t, err)
}

func TestDecodeFindCoordinatorReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f290005746f706963")
	findCoordinatorReq, err := DecodeFindCoordinatorReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, findCoordinatorReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", findCoordinatorReq.ClientId)
	assert.Equal(t, "topic", findCoordinatorReq.Key)
	var expectKeyType uint8 = 0
	assert.Equal(t, expectKeyType, findCoordinatorReq.KeyType)
}

func TestDecodeFindCoordinatorReqV3(t *testing.T) {
	bytes := testHex2Bytes(t, "00000000002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d31002537336664633964612d306439322d346537622d613761372d6563323636663637633137310000")
	findCoordinatorReq, err := DecodeFindCoordinatorReq(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, 0, findCoordinatorReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", findCoordinatorReq.ClientId)
	assert.Equal(t, "73fdc9da-0d92-4e7b-a7a7-ec266f67c171", findCoordinatorReq.Key)
	var expectKeyType uint8 = 0
	assert.Equal(t, expectKeyType, findCoordinatorReq.KeyType)
}

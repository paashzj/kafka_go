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

func TestDecodeIllegalSyncGroupReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeSyncGroupReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeSyncGroupReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000003006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f290005746f7069630000000300925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d3261326665653639383963380000000100925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d326132666565363938396338000000190001000000010005746f7069630000000100000000ffffffff")
	fetchReq, err := DecodeSyncGroupReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, 3, fetchReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", fetchReq.ClientId)
	assert.Equal(t, "topic", fetchReq.GroupId)
	assert.Equal(t, 3, fetchReq.GenerationId)
	assert.Equal(t, "topic", fetchReq.GroupId)
	assert.Len(t, fetchReq.GroupAssignments, 1)
	groupAssignment := fetchReq.GroupAssignments[0]
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8", groupAssignment.MemberId)
}

func TestDecodeSyncGroupReqV4(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31002538646437623936622d366239342d346139622d623263632d3363623538393863396364660000000155636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d366132343963666630376663000255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766631b0001000000010006746573742d350000000100000000ffffffff0000")
	fetchReq, err := DecodeSyncGroupReq(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, 6, fetchReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", fetchReq.ClientId)
	assert.Equal(t, "8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf", fetchReq.GroupId)
	assert.Equal(t, 1, fetchReq.GenerationId)
	assert.Equal(t, "8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf", fetchReq.GroupId)
	assert.Equal(t, "", fetchReq.ProtocolType)
	assert.Equal(t, "", fetchReq.ProtocolName)
	assert.Len(t, fetchReq.GroupAssignments, 1)
	groupAssignment := fetchReq.GroupAssignments[0]
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", groupAssignment.MemberId)
}

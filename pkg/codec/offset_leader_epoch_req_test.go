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

func TestDecodeOffsetLeaderEpochReqV3(t *testing.T) {
	bytes := testHex2Bytes(t, "000000090015636f6e73756d65722d6c742d67726f75702d312d31ffffffff0000000100096c742d746573742d3100000001000000000000000000000000")
	leaderEpochReq, err := DecodeOffsetLeaderEpochReq(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, 9, leaderEpochReq.CorrelationId)
	var expectedReplicaId int32 = -1
	assert.Equal(t, expectedReplicaId, leaderEpochReq.ReplicaId)
	assert.Len(t, leaderEpochReq.TopicReqList, 1)
	leaderEpochTopicReq := leaderEpochReq.TopicReqList[0]
	assert.Equal(t, "lt-test-1", leaderEpochTopicReq.Topic)
	leaderEpochPartitionReq := leaderEpochTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, leaderEpochPartitionReq.PartitionId)
	var expectedLeaderEpoch int32 = 0
	assert.Equal(t, expectedLeaderEpoch, leaderEpochPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, expectedLeaderEpoch, leaderEpochPartitionReq.LeaderEpoch)
}

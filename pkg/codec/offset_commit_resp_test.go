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

func TestCodeOffsetCommitRespV2(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "topic"
	offsetCommitTopicResp.PartitionRespList = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := NewOffsetCommitResp(5)
	offsetCommitResp.TopicRespList = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(2)
	expectBytes := testHex2Bytes(t, "00000005000000010005746f70696300000001000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeOffsetCommitRespV8(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "test-5"
	offsetCommitTopicResp.PartitionRespList = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := NewOffsetCommitResp(11)
	offsetCommitResp.TopicRespList = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(8)
	expectBytes := testHex2Bytes(t, "0000000b00000000000207746573742d3502000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

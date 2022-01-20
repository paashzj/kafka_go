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

func TestCodeListOffsetsRespV1(t *testing.T) {
	listOffsetPartitionResp := &ListOffsetPartitionResp{}
	listOffsetPartitionResp.PartitionId = 0
	listOffsetPartitionResp.ErrorCode = 0
	listOffsetPartitionResp.Timestamp = -1
	listOffsetPartitionResp.Offset = 1
	listOffsetTopicResp := &ListOffsetTopicResp{}
	listOffsetTopicResp.Topic = "topic"
	listOffsetTopicResp.ListOffsetPartitions = []*ListOffsetPartitionResp{listOffsetPartitionResp}
	listOffsetResp := NewListOffsetResp(4)
	listOffsetResp.OffsetTopics = []*ListOffsetTopicResp{listOffsetTopicResp}
	bytes := listOffsetResp.Bytes(1)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f70696300000001000000000000ffffffffffffffff0000000000000001")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeListOffsetsRespV5(t *testing.T) {
	listOffsetPartitionResp := &ListOffsetPartitionResp{}
	listOffsetPartitionResp.PartitionId = 0
	listOffsetPartitionResp.ErrorCode = 0
	listOffsetPartitionResp.Timestamp = -1
	listOffsetPartitionResp.Offset = 0
	listOffsetPartitionResp.LeaderEpoch = 0
	listOffsetTopicResp := &ListOffsetTopicResp{}
	listOffsetTopicResp.Topic = "test-5"
	listOffsetTopicResp.ListOffsetPartitions = []*ListOffsetPartitionResp{listOffsetPartitionResp}
	listOffsetResp := NewListOffsetResp(8)
	listOffsetResp.OffsetTopics = []*ListOffsetTopicResp{listOffsetTopicResp}
	bytes := listOffsetResp.Bytes(5)
	expectBytes := testHex2Bytes(t, "0000000800000000000000010006746573742d3500000001000000000000ffffffffffffffff000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

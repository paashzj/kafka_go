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

func TestCodeOffsetFetchRespV1(t *testing.T) {
	offsetFetchResp := NewOffsetFetchResp(4)
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "topic"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(1)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f7069630000000100000000ffffffffffffffff00000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeOffsetFetchRespV6(t *testing.T) {
	offsetFetchResp := NewOffsetFetchResp(7)
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	offsetFetchPartitionResp.LeaderEpoch = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "test-5"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(6)
	expectBytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	assert.Equal(t, expectBytes, bytes)
}

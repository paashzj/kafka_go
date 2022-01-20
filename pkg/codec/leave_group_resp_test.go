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

func TestCodeLeaveGroupRespV0(t *testing.T) {
	leaveGroupResp := NewLeaveGroupResp(1)
	bytes := leaveGroupResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "000000010000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeLeaveGroupRespV4(t *testing.T) {
	leaveGroupMember := &LeaveGroupMember{}
	leaveGroupMember.MemberId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc"
	leaveGroupResp := NewLeaveGroupResp(12)
	leaveGroupResp.Members = []*LeaveGroupMember{leaveGroupMember}
	bytes := leaveGroupResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "0000000c000000000000000255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766630000000000")
	assert.Equal(t, expectBytes, bytes)
}

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

func TestCodeSyncGroupRespV0(t *testing.T) {
	syncGroupResp := NewSyncGroupResp(3)
	syncGroupResp.ErrorCode = 0
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010005746f7069630000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "000000030000000000190001000000010005746f7069630000000100000000ffffffff")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeSyncGroupRespV4(t *testing.T) {
	syncGroupResp := NewSyncGroupResp(6)
	syncGroupResp.ProtocolType = ""
	syncGroupResp.ProtocolName = ""
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "00000006000000000000001b0001000000010006746573742d350000000100000000ffffffff00")
	assert.Equal(t, expectBytes, bytes)
}

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

func TestCodeApiVersionRespV0(t *testing.T) {
	apiVersionResp := NewApiVersionResp(1)
	bytes := apiVersionResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "0000000100000000001400000000000900010000000c00020000000600030000000b000800000008000900000007000a00000003000b00000007000c00000004000d00000004000e00000005000f00000005001000000004001100000001001200000003001300000007001400000006001500000002001700000004002400000002")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeApiVersionRespV3(t *testing.T) {
	apiVersionResp := NewApiVersionResp(1)
	bytes := apiVersionResp.Bytes(3)
	expectBytes := testHex2Bytes(t, "000000010000150000000000090000010000000c000002000000060000030000000b000008000000080000090000000700000a0000000300000b0000000700000c0000000400000d0000000400000e0000000500000f000000050000100000000400001100000001000012000000030000130000000700001400000006000015000000020000170000000400002400000002000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

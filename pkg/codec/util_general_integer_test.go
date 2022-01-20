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

func TestReadInt64Case1(t *testing.T) {
	timestampSlice := []byte{0x00, 0x00, 0x01, 0x7a, 0x92, 0xe3, 0x83, 0xdd}
	res, _ := readInt64(timestampSlice, 0)
	var expected int64 = 1625962021853
	assert.Equal(t, expected, res)
}

func TestReadInt64Case2(t *testing.T) {
	timestampSlice := testHex2Bytes(t, "0000017a931dccdf")
	res, _ := readInt64(timestampSlice, 0)
	var expected int64 = 1625965841631
	assert.Equal(t, expected, res)
}

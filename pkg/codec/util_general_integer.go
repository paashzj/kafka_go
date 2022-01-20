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

import "encoding/binary"

// int

func readInt64(bytes []byte, idx int) (int64, int) {
	return int64(binary.BigEndian.Uint64(bytes[idx:])), idx + 8
}

func putInt64(bytes []byte, idx int, x int64) int {
	binary.BigEndian.PutUint64(bytes[idx:idx+8], uint64(x))
	return idx + 8
}

func readInt(bytes []byte, idx int) (int, int) {
	return int(binary.BigEndian.Uint32(bytes[idx:])), idx + 4
}

func putInt(bytes []byte, idx int, x int) int {
	return putUInt32(bytes, idx, uint32(x))
}

func readInt32(bytes []byte, idx int) (int32, int) {
	return int32(binary.BigEndian.Uint32(bytes[idx:])), idx + 4
}

func putInt32(bytes []byte, idx int, x int32) int {
	return putUInt32(bytes, idx, uint32(x))
}

func readInt16(bytes []byte, idx int) (int16, int) {
	return int16(binary.BigEndian.Uint16(bytes[idx:])), idx + 2
}

func putInt16(bytes []byte, idx int, x int16) int {
	return putUInt16(bytes, idx, uint16(x))
}

// uint

func putUInt64(bytes []byte, idx int, x uint64) int {
	binary.BigEndian.PutUint64(bytes[idx:idx+8], x)
	return idx + 8
}

func readUInt64(bytes []byte, idx int) (uint64, int) {
	return binary.BigEndian.Uint64(bytes[idx:]), idx + 8
}

func putUInt32(bytes []byte, idx int, x uint32) int {
	binary.BigEndian.PutUint32(bytes[idx:idx+4], x)
	return idx + 4
}

func readUInt32(bytes []byte, idx int) (uint32, int) {
	return binary.BigEndian.Uint32(bytes[idx:]), idx + 4
}

func putUInt16(bytes []byte, idx int, x uint16) int {
	binary.BigEndian.PutUint16(bytes[idx:idx+2], x)
	return idx + 2
}

func readUInt16(bytes []byte, idx int) (uint16, int) {
	return binary.BigEndian.Uint16(bytes[idx:]), idx + 2
}

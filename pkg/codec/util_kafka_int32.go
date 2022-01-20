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

// This file is for kafka code int32 type. Format method as alpha order.

func putBaseSequence(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readBaseSequence(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putBrokerNodeId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readBrokerNodeId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putControllerId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readControllerId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putFetchSessionEpoch(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readFetchSessionEpoch(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putLeaderEpoch(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readLeaderEpoch(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putLeaderId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readLeaderId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putNodeId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readNodeId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putReplicaId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readReplicaId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

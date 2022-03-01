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

// This file is for kafka code int type. Format method as alpha order.

func putBrokerPort(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readBrokerPort(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putClusterAuthorizedOperation(bytes []byte, idx int, corrId int) int {
	return putInt(bytes, idx, corrId)
}

func putCorrId(bytes []byte, idx int, corrId int) int {
	return putInt(bytes, idx, corrId)
}

func readCorrId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putGenerationId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readGenerationId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putLastOffsetDelta(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readLastOffsetDelta(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putMessageSize(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readMessageSize(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putPartitionId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readPartitionId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putSessionId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readSessionId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putThrottleTime(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readThrottleTime(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putTopicAuthorizedOperation(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

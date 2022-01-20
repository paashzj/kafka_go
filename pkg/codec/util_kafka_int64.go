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

// This file is for kafka code int64 type. Format method as alpha order.

func putLogStartOffset(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readLogStartOffset(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putOffset(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readOffset(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putProducerId(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readProducerId(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putRetentionTime(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readRetentionTime(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putSessionLifeTimeout(bytes []byte, idx int, ms int64) int {
	return putInt64(bytes, idx, ms)
}

func putTime(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readTime(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

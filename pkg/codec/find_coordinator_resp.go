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

type FindCoordinatorResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
	ErrorMessage *string
	NodeId       int32
	Host         string
	Port         int
}

func NewFindCoordinatorResp(corrId int, config *KafkaProtocolConfig) *FindCoordinatorResp {
	findCoordinatorResp := FindCoordinatorResp{}
	findCoordinatorResp.CorrelationId = corrId
	findCoordinatorResp.NodeId = config.NodeId
	findCoordinatorResp.Host = config.AdvertiseHost
	findCoordinatorResp.Port = config.AdvertisePort
	return &findCoordinatorResp
}

func (f *FindCoordinatorResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 3 {
		result += LenTaggedField + LenThrottleTime
	}
	result += LenErrorCode
	if version == 3 {
		result += CompactNullableStrLen(f.ErrorMessage)
	}
	result += LenNodeId
	if version == 0 {
		result += StrLen(f.Host)
	} else if version == 3 {
		result += CompactStrLen(f.Host)
	}
	result += LenPort
	if version == 3 {
		result += LenTaggedField
	}
	return result
}

func (f *FindCoordinatorResp) Bytes(version int16) []byte {
	bytes := make([]byte, f.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, f.CorrelationId)
	if version == 3 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, 0)
	}
	idx = putErrorCode(bytes, idx, f.ErrorCode)
	if version == 3 {
		idx = putFindCoordinatorErrorMessage(bytes, idx, f.ErrorMessage)
	}
	idx = putNodeId(bytes, idx, f.NodeId)
	if version == 0 {
		idx = putHostString(bytes, idx, f.Host)
	} else if version == 3 {
		idx = putHost(bytes, idx, f.Host)
	}
	idx = putInt(bytes, idx, f.Port)
	if version == 3 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

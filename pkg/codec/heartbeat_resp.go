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

import "github.com/paashzj/kafka_go/pkg/service"

type HeartBeatResp struct {
	BaseResp
	ErrorCode    int16
	ThrottleTime int
}

func NewHeartBeatResp(corrId int) *HeartBeatResp {
	beatResp := HeartBeatResp{}
	beatResp.CorrelationId = corrId
	return &beatResp
}

func NewHeartBeatRespWithErr(corrId int, code service.ErrorCode) *HeartBeatResp {
	beatResp := HeartBeatResp{}
	beatResp.CorrelationId = corrId
	beatResp.ErrorCode = int16(code)
	return &beatResp
}

func (h *HeartBeatResp) BytesLength(version int16) int {
	return LenCorrId + LenTaggedField + LenThrottleTime + LenErrorCode + LenTaggedField
}

func (h *HeartBeatResp) Bytes(version int16) []byte {
	bytes := make([]byte, h.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, h.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, 0)
	idx = putErrorCode(bytes, idx, h.ErrorCode)
	idx = putTaggedField(bytes, idx)
	return bytes
}

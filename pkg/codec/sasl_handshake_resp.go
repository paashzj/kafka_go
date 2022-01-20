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

type SaslHandshakeResp struct {
	BaseResp
	ErrorCode        int16
	EnableMechanisms []*EnableMechanism
}

type EnableMechanism struct {
	SaslMechanism string
}

func NewSaslHandshakeResp(corrId int) *SaslHandshakeResp {
	saslHandshakeResp := SaslHandshakeResp{}
	saslHandshakeResp.CorrelationId = corrId
	return &saslHandshakeResp
}

func (s *SaslHandshakeResp) BytesLength(version int16) int {
	length := LenCorrId + LenErrorCode + LenArray
	for _, val := range s.EnableMechanisms {
		length += StrLen(val.SaslMechanism)
	}
	return length
}

// Bytes 转化为字节数组
func (s *SaslHandshakeResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putErrorCode(bytes, idx, s.ErrorCode)
	idx = putArrayLen(bytes, idx, len(s.EnableMechanisms))
	for _, val := range s.EnableMechanisms {
		idx = putSaslMechanism(bytes, idx, val.SaslMechanism)
	}
	return bytes
}

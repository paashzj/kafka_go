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

func TestDecodeIllegalSaslHandshakeReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeSaslHandshakeReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeSaslHandshakeReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffff9002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d310005504c41494e")
	saslHandshakeReq, err := DecodeSaslHandshakeReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 2147483641, saslHandshakeReq.CorrelationId)
	assert.Equal(t, "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1", saslHandshakeReq.ClientId)
	assert.Equal(t, "PLAIN", saslHandshakeReq.SaslMechanism)
}

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
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCodeHeartbeatRespV4(t *testing.T) {
	heartBeatResp := NewHeartBeatResp(17)
	bytes := heartBeatResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "000000110000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeHeartbeatRespWithErrV4(t *testing.T) {
	heartBeatResp := NewHeartBeatRespWithErr(17, service.REBALANCE_IN_PROGRESS)
	bytes := heartBeatResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "000000110000000000001b00")
	assert.Equal(t, expectBytes, bytes)
}

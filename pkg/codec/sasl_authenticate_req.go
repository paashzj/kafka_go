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
	"errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type SaslAuthenticateReq struct {
	BaseReq
	Username string
	Password string
}

func DecodeSaslHandshakeAuthReq(bytes []byte, version int16) (authReq *SaslAuthenticateReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered in f", r, string(debug.Stack()))
			authReq = nil
			err = errors.New("codec failed")
		}
	}()
	authReq = &SaslAuthenticateReq{}
	idx := 0
	authReq.CorrelationId, idx = readCorrId(bytes, idx)
	authReq.ClientId, idx = readClientId(bytes, idx)
	var saslBytes []byte
	if version == 1 {
		saslBytes, idx = readBytes(bytes, idx)
	} else if version == 2 {
		idx = readTaggedField(bytes, idx)
		saslBytes, idx = readCompactBytes(bytes, idx)
	}
	authReq.Username, authReq.Password = readSaslAuthBytes(saslBytes, 0)
	if err != nil {
		return nil, errors.New("can't decode username or password")
	}
	if version == 2 {
		idx = readTaggedField(bytes, idx)
	}
	return authReq, nil
}

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

type FindCoordinatorReq struct {
	BaseReq
	Key     string
	KeyType byte
}

func DecodeFindCoordinatorReq(bytes []byte, version int16) (findCoordinatorReq *FindCoordinatorReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			findCoordinatorReq = nil
			err = errors.New("codec failed")
		}
	}()
	findCoordinatorReq = &FindCoordinatorReq{}
	idx := 0
	findCoordinatorReq.CorrelationId, idx = readCorrId(bytes, idx)
	findCoordinatorReq.ClientId, idx = readClientId(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		findCoordinatorReq.Key, idx = readString(bytes, idx)
	} else if version == 3 {
		findCoordinatorReq.Key, idx = readCoordinatorKey(bytes, idx)
	}
	if version == 0 {
		findCoordinatorReq.KeyType = 0
	} else if version == 3 {
		findCoordinatorReq.KeyType, idx = readCoordinatorType(bytes, idx)
	}
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	return findCoordinatorReq, nil
}

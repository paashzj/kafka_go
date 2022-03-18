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

type MetadataReq struct {
	BaseReq
	Topics                             []*MetadataTopicReq
	AllowAutoTopicCreation             bool
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
}

type MetadataTopicReq struct {
	Topic string
}

func DecodeMetadataTopicReq(bytes []byte, version int16) (metadataReq *MetadataReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered in f", r, string(debug.Stack()))
			metadataReq = nil
			err = errors.New("codec failed")
		}
	}()
	metadataReq = &MetadataReq{}
	idx := 0
	metadataReq.CorrelationId, idx = readCorrId(bytes, idx)
	metadataReq.ClientId, idx = readClientId(bytes, idx)
	if version == 9 {
		idx = readTaggedField(bytes, idx)
	}
	var length int
	if version == 1 {
		length, idx = readArrayLen(bytes, idx)
	} else {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	metadataReq.Topics = make([]*MetadataTopicReq, length)
	for i := 0; i < length; i++ {
		metadataTopicReq := MetadataTopicReq{}
		if version == 1 {
			metadataTopicReq.Topic, idx = readTopicString(bytes, idx)
		} else if version == 9 {
			metadataTopicReq.Topic, idx = readTopic(bytes, idx)
			readTaggedField(bytes, idx)
		}
		metadataReq.Topics[i] = &metadataTopicReq
	}
	return metadataReq, nil
}

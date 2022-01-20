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

type ProduceReq struct {
	BaseReq
	ClientId      string
	TransactionId int16
	RequiredAcks  int16
	Timeout       int
	TopicReqList  []*ProduceTopicReq
}

type ProduceTopicReq struct {
	Topic            string
	PartitionReqList []*ProducePartitionReq
}

type ProducePartitionReq struct {
	PartitionId int
	RecordBatch *RecordBatch
}

func DecodeProduceReq(bytes []byte, version int16) (produceReq *ProduceReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in f", r, string(debug.Stack()))
			produceReq = nil
			err = errors.New("codec failed")
		}
	}()
	produceReq = &ProduceReq{}
	idx := 0
	produceReq.CorrelationId, idx = readCorrId(bytes, idx)
	produceReq.ClientId, idx = readClientId(bytes, idx)
	// todo skip transactionId
	idx += 2
	// todo skip requiredAcks
	idx += 2
	produceReq.Timeout, idx = readInt(bytes, idx)
	var length int
	length, idx = readInt(bytes, idx)
	produceReq.TopicReqList = make([]*ProduceTopicReq, length)
	for i := 0; i < length; i++ {
		topic := &ProduceTopicReq{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx = readInt(bytes, idx)
		topic.PartitionReqList = make([]*ProducePartitionReq, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &ProducePartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			recordBatchLength, idx := readInt(bytes, idx)
			partition.RecordBatch = DecodeRecordBatch(bytes[idx:idx+recordBatchLength-1], version)
			idx += recordBatchLength
			topic.PartitionReqList[i] = partition
		}
		produceReq.TopicReqList[i] = topic
	}
	return produceReq, nil
}

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

func TestCodeRecordBatch(t *testing.T) {
	r := &RecordBatch{}
	r.Offset = 0
	r.MessageSize = 64
	r.LeaderEpoch = 0
	r.MagicByte = 2
	r.Flags = 0
	r.LastOffsetDelta = 0
	r.FirstTimestamp = 1625962021853
	r.LastTimestamp = 1625962021853
	r.ProducerId = -1
	r.ProducerEpoch = -1
	r.BaseSequence = -1
	r.Records = make([]*Record, 1)
	record := &Record{}
	record.RecordAttributes = 0
	record.RelativeTimestamp = 0
	record.RelativeOffset = 0
	record.Key = nil
	record.Value = []byte("ShootHzj")
	r.Records[0] = record
	r.Bytes()
	assert.Equal(t, 76, r.BytesLength())
}

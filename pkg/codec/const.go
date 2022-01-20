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

const (
	LenCorrId               = 4
	LenErrorCode            = 2
	LenArray                = 4
	LenApiV0                = 6
	LenApiV3                = 7
	LenThrottleTime         = 4
	LenFetchSessionId       = 4
	LenPartitionId          = 4
	LenOffset               = 8
	LenLastStableOffset     = 8
	LenStartOffset          = 8
	LenAbortTransactions    = 4
	LenReplicaId            = 4
	LenMessageSize          = 4
	LenNodeId               = 4
	LenPort                 = 4
	LenGenerationId         = 4
	LenTime                 = 8
	LenLeaderId             = 4
	LenLeaderEpoch          = 4
	LenControllerId         = 4
	LenIsInternalV9         = 4
	LenIsInternalV1         = 1
	LenTopicAuthOperation   = 4
	LenClusterAuthOperation = 4
	LenRecordAttributes     = 1
	LenMagicByte            = 1
	LenCrc32                = 4
	LenOffsetDelta          = 4
	LenProducerId           = 8
	LenProducerEpoch        = 2
	LenBaseSequence         = 4
	LenSessionTimeout       = 8
)

const LenTaggedField = 1

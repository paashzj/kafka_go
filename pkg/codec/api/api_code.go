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

package api

type Code int16

const (
	Produce Code = iota
	Fetch
	ListOffsets
	Metadata
	LeaderAndIsr
	StopReplica
	UpdateMetadata
	ControlledShutdown
	OffsetCommit
	OffsetFetch
	FindCoordinator
	JoinGroup
	Heartbeat
	LeaveGroup
	SyncGroup
	DescribeGroups
	ListGroups
	SaslHandshake
	ApiVersions
	CreateTopics
	DeleteTopics
	DeleteRecords
	InitProducerId
	OffsetForLeaderEpoch
	AddPartitionsToTxn
	AddOffsetsToTxn
	EndTxn
	WriteTxnMarkers
	TxnOffsetCommit
	DescribeAcls
	CreateAcls
	DeleteAcls
	DescribeConfigs
	AlterConfigs
	AlterReplicaLogDirs
	DescribeLogDirs
	SaslAuthenticate
	CreatePartitions
	CreateDelegationToken
	RenewDelegationToken
	ExpireDelegationToken
	DescribeDelegationToken
	DeleteGroups
	ElectLeaders
	IncrementalAlterConfigs
	AlterPartitionReassignments
	ListPartitionReassignments
	OffsetDelete
	DescribeClientQuotas
	AlterClientQuotas
	DescribeUserScramCredentials
	AlterUserScramCredentials
	AlterIsr
	UpdateFeatures
	DescribeCluster
	DescribeProducers
)

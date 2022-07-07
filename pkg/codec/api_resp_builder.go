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
	"github.com/paashzj/kafka_go/pkg/codec/api"
)

func buildApiRespVersions() []*ApiRespVersion {
	apiRespVersions := make([]*ApiRespVersion, 20)
	apiRespVersions[0] = &ApiRespVersion{ApiKey: api.Produce, MinVersion: 0, MaxVersion: 9}
	apiRespVersions[1] = &ApiRespVersion{ApiKey: api.Fetch, MinVersion: 0, MaxVersion: 12}
	apiRespVersions[2] = &ApiRespVersion{ApiKey: api.ListOffsets, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[3] = &ApiRespVersion{ApiKey: api.Metadata, MinVersion: 0, MaxVersion: 11}
	apiRespVersions[4] = &ApiRespVersion{ApiKey: api.OffsetCommit, MinVersion: 0, MaxVersion: 8}
	apiRespVersions[5] = &ApiRespVersion{ApiKey: api.OffsetFetch, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[6] = &ApiRespVersion{ApiKey: api.FindCoordinator, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[7] = &ApiRespVersion{ApiKey: api.JoinGroup, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[8] = &ApiRespVersion{ApiKey: api.Heartbeat, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[9] = &ApiRespVersion{ApiKey: api.LeaveGroup, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[10] = &ApiRespVersion{ApiKey: api.SyncGroup, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[11] = &ApiRespVersion{ApiKey: api.DescribeGroups, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[12] = &ApiRespVersion{ApiKey: api.ListGroups, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[13] = &ApiRespVersion{ApiKey: api.SaslHandshake, MinVersion: 0, MaxVersion: 1}
	apiRespVersions[14] = &ApiRespVersion{ApiKey: api.ApiVersions, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[15] = &ApiRespVersion{ApiKey: api.CreateTopics, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[16] = &ApiRespVersion{ApiKey: api.DeleteTopics, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[17] = &ApiRespVersion{ApiKey: api.DeleteRecords, MinVersion: 0, MaxVersion: 2}
	apiRespVersions[18] = &ApiRespVersion{ApiKey: api.OffsetForLeaderEpoch, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[19] = &ApiRespVersion{ApiKey: api.SaslAuthenticate, MinVersion: 0, MaxVersion: 2}
	return apiRespVersions
}

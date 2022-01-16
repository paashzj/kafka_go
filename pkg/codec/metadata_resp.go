package codec

type MetadataResp struct {
	BaseResp
	ThrottleTime               int
	ErrorCode                  int16
	BrokerMetadataList         []*BrokerMetadata
	ClusterId                  string
	ControllerId               int32
	TopicMetadataList          []*TopicMetadata
	ClusterAuthorizedOperation int
}

type BrokerMetadata struct {
	NodeId int32
	Host   string
	Port   int
	Rack   *string
}

type TopicMetadata struct {
	ErrorCode                int16
	Topic                    string
	IsInternal               bool
	PartitionMetadataList    []*PartitionMetadata
	TopicAuthorizedOperation int
}

type PartitionMetadata struct {
	ErrorCode       int16
	PartitionId     int
	LeaderId        int32
	LeaderEpoch     int32
	Replicas        []*Replica
	CaughtReplicas  []*Replica
	OfflineReplicas []*Replica
}

type Replica struct {
	ReplicaId int32
}

func NewMetadataResp(corrId int, config *KafkaProtocolConfig, topicName string, errorCode int16) *MetadataResp {
	metadataResp := MetadataResp{}
	metadataResp.CorrelationId = corrId
	metadataResp.BrokerMetadataList = make([]*BrokerMetadata, 1)
	metadataResp.BrokerMetadataList[0] = &BrokerMetadata{NodeId: config.NodeId, Host: config.AdvertiseHost, Port: config.AdvertisePort, Rack: nil}
	metadataResp.ClusterId = config.ClusterId
	metadataResp.ControllerId = config.NodeId
	metadataResp.TopicMetadataList = make([]*TopicMetadata, 1)
	topicMetadata := TopicMetadata{ErrorCode: errorCode, Topic: topicName, IsInternal: false, TopicAuthorizedOperation: -2147483648}
	topicMetadata.PartitionMetadataList = make([]*PartitionMetadata, 1)
	partitionMetadata := &PartitionMetadata{ErrorCode: 0, PartitionId: 0, LeaderId: config.NodeId, LeaderEpoch: 0, OfflineReplicas: nil}
	replicas := make([]*Replica, 1)
	replicas[0] = &Replica{ReplicaId: int32(config.NodeId)}
	partitionMetadata.Replicas = replicas
	partitionMetadata.CaughtReplicas = replicas
	topicMetadata.PartitionMetadataList[0] = partitionMetadata
	metadataResp.TopicMetadataList[0] = &topicMetadata
	metadataResp.ClusterAuthorizedOperation = -2147483648
	return &metadataResp
}

func (m *MetadataResp) BytesLength(version int16) int {
	// 4字节CorrId + 1字节TaggedField + 4字节 ThrottleTime + 1字节Broker metadata长度
	result := LenCorrId
	if version == 9 {
		result += LenTaggedField
		result += LenThrottleTime
	}
	if version == 9 {
		result += varintSize(len(m.BrokerMetadataList) + 1)
	} else if version == 1 {
		result += LenArray
	}
	for _, val := range m.BrokerMetadataList {
		result += LenNodeId
		if version == 9 {
			result += CompactStrLen(val.Host)
		} else if version == 1 {
			result += StrLen(val.Host)
		}
		result += LenPort
		if version == 9 {
			result += CompactNullableStrLen(val.Rack)
		} else if version == 1 {
			// todo
			result += 2
		}
		if version == 9 {
			result += LenTaggedField
		}
	}
	if version == 9 {
		result += CompactStrLen(m.ClusterId)
	}
	result += LenControllerId
	if version == 9 {
		result += varintSize(len(m.TopicMetadataList) + 1)
	} else if version == 1 {
		result += LenArray
	}
	for _, val := range m.TopicMetadataList {
		// is internal + 数组长度
		result += LenErrorCode
		if version == 9 {
			result += CompactStrLen(val.Topic)
		} else if version == 1 {
			result += StrLen(val.Topic)
		}
		if version == 9 {
			result += LenIsInternalV9
		} else if version == 1 {
			result += LenIsInternalV1
		}
		if version == 9 {
			result += varintSize(len(val.PartitionMetadataList) + 1)
		} else if version == 1 {
			result += LenArray
		}
		for _, partition := range val.PartitionMetadataList {
			result += LenErrorCode + LenPartitionId + LenLeaderId
			if version == 9 {
				result += LenLeaderEpoch
			}
			if version == 9 {
				result += varintSize(len(partition.Replicas) + 1)
			} else if version == 1 {
				result += LenArray
			}
			for range partition.Replicas {
				result += LenReplicaId
			}
			if version == 9 {
				result += varintSize(len(partition.CaughtReplicas) + 1)
			} else if version == 1 {
				result += LenArray
			}
			for range partition.CaughtReplicas {
				result += LenReplicaId
			}
			if version == 9 {
				result += varintSize(len(partition.OfflineReplicas) + 1)
				for range partition.OfflineReplicas {
					result += LenReplicaId
				}
				result += LenTaggedField
			}
		}
		if version == 9 {
			result += LenTopicAuthOperation
		}
		if version == 9 {
			result += LenTaggedField
		}
	}
	if version == 9 {
		result += LenClusterAuthOperation
		result += LenTaggedField
	}
	return result
}

func (m *MetadataResp) Bytes(version int16) []byte {
	bytes := make([]byte, m.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, m.CorrelationId)
	if version == 9 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, 0)
	}
	if version == 9 {
		idx = putVarint(bytes, idx, len(m.BrokerMetadataList))
	} else if version == 1 {
		idx = putInt(bytes, idx, len(m.BrokerMetadataList))
	}
	for _, brokerMetadata := range m.BrokerMetadataList {
		idx = putBrokerNodeId(bytes, idx, brokerMetadata.NodeId)
		if version == 9 {
			idx = putHost(bytes, idx, brokerMetadata.Host)
		} else if version == 1 {
			idx = putHostString(bytes, idx, brokerMetadata.Host)
		}
		idx = putBrokerPort(bytes, idx, brokerMetadata.Port)
		if version == 9 {
			idx = putRack(bytes, idx, brokerMetadata.Rack)
		} else {
			// todo
			//idx += 2
			bytes[idx] = 255
			idx += 1
			bytes[idx] = 255
			idx += 1
		}
		if version == 9 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 9 {
		idx = putClusterId(bytes, idx, m.ClusterId)
	}
	idx = putControllerId(bytes, idx, m.ControllerId)
	if version == 9 {
		idx = putVarint(bytes, idx, len(m.TopicMetadataList))
	} else if version == 1 {
		idx = putInt(bytes, idx, len(m.TopicMetadataList))
	}
	for _, topicMetadata := range m.TopicMetadataList {
		idx = putErrorCode(bytes, idx, topicMetadata.ErrorCode)
		if version == 9 {
			idx = putTopic(bytes, idx, topicMetadata.Topic)
		} else {
			idx = putTopicString(bytes, idx, topicMetadata.Topic)
		}
		idx = putBool(bytes, idx, topicMetadata.IsInternal)
		if version == 9 {
			idx = putVarint(bytes, idx, len(topicMetadata.PartitionMetadataList))
		} else if version == 1 {
			idx = putInt(bytes, idx, len(topicMetadata.PartitionMetadataList))
		}
		for _, partitionMetadata := range topicMetadata.PartitionMetadataList {
			idx = putErrorCode(bytes, idx, partitionMetadata.ErrorCode)
			idx = putPartitionId(bytes, idx, partitionMetadata.PartitionId)
			idx = putLeaderId(bytes, idx, partitionMetadata.LeaderId)
			if version == 9 {
				idx = putLeaderEpoch(bytes, idx, partitionMetadata.LeaderEpoch)
			}
			if version == 9 {
				idx = putVarint(bytes, idx, len(partitionMetadata.Replicas))
			} else if version == 1 {
				idx = putInt(bytes, idx, len(partitionMetadata.Replicas))
			}
			for _, replica := range partitionMetadata.Replicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			if version == 9 {
				idx = putVarint(bytes, idx, len(partitionMetadata.CaughtReplicas))
			} else if version == 1 {
				idx = putInt(bytes, idx, len(partitionMetadata.CaughtReplicas))
			}
			for _, replica := range partitionMetadata.CaughtReplicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			if version == 9 {
				bytes[idx] = byte(len(partitionMetadata.OfflineReplicas) + 1)
				idx++
				for _, replica := range partitionMetadata.OfflineReplicas {
					idx = putReplicaId(bytes, idx, replica.ReplicaId)
				}
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 9 {
			idx = putTopicAuthorizedOperation(bytes, idx, topicMetadata.TopicAuthorizedOperation)
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 9 {
		idx = putClusterAuthorizedOperation(bytes, idx, m.ClusterAuthorizedOperation)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

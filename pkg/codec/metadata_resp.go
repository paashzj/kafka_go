package codec

type MetadataResp struct {
	BaseResp
	ThrottleTime               int
	ErrorCode                  int16
	BrokerMetadataList         []*BrokerMetadata
	ClusterId                  string
	ControllerId               int
	TopicMetadataList          []*TopicMetadata
	ClusterAuthorizedOperation int
}

type BrokerMetadata struct {
	NodeId int
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
	LeaderId        int
	LeaderEpoch     int
	Replicas        []*Replica
	CaughtReplicas  []*Replica
	OfflineReplicas []*Replica
}

type Replica struct {
	ReplicaId int
}

func NewMetadataResp(corrId int, config *KafkaProtocolConfig, topicName string, errorCode int16) *MetadataResp {
	metadataResp := MetadataResp{}
	metadataResp.CorrelationId = corrId
	metadataResp.BrokerMetadataList = make([]*BrokerMetadata, 1)
	metadataResp.BrokerMetadataList[0] = &BrokerMetadata{NodeId: 0, Host: config.AdvertiseHost, Port: config.AdvertisePort, Rack: nil}
	metadataResp.ClusterId = config.ClusterId
	metadataResp.ControllerId = 0
	metadataResp.TopicMetadataList = make([]*TopicMetadata, 1)
	topicMetadata := TopicMetadata{ErrorCode: errorCode, Topic: topicName, IsInternal: false, TopicAuthorizedOperation: -2147483648}
	topicMetadata.PartitionMetadataList = make([]*PartitionMetadata, 1)
	partitionMetadata := &PartitionMetadata{ErrorCode: 0, PartitionId: 0, LeaderId: 0, LeaderEpoch: 0, OfflineReplicas: nil}
	replicas := make([]*Replica, 1)
	replicas[0] = &Replica{ReplicaId: 0}
	partitionMetadata.Replicas = replicas
	partitionMetadata.CaughtReplicas = replicas
	topicMetadata.PartitionMetadataList[0] = partitionMetadata
	metadataResp.TopicMetadataList[0] = &topicMetadata
	metadataResp.ClusterAuthorizedOperation = -2147483648
	return &metadataResp
}

func (m *MetadataResp) BytesLength(version int16) int {
	// 4字节CorrId + 1字节TaggedField + 4字节 ThrottleTime + 1字节Broker metadata长度
	result := LenCorrId + LenTaggedField + LenThrottleTime + varintSize(len(m.BrokerMetadataList)+1)
	for _, val := range m.BrokerMetadataList {
		result += LenNodeId + CompactStrLen(val.Host) + LenGenerationId + CompactNullableStrLen(val.Rack) + LenTaggedField
	}
	result += CompactStrLen(m.ClusterId)
	result += LenControllerId + varintSize(len(m.TopicMetadataList)+1)
	for _, val := range m.TopicMetadataList {
		// is internal + 数组长度
		result += LenErrorCode + CompactStrLen(val.Topic) + LenIsInternal + varintSize(len(val.PartitionMetadataList)+1)
		for _, partition := range val.PartitionMetadataList {
			result += LenErrorCode + LenPartitionId + LenLeaderId + LenLeaderEpoch
			result += varintSize(len(partition.Replicas) + 1)
			for range partition.Replicas {
				result += LenReplicaId
			}
			result += varintSize(len(partition.CaughtReplicas) + 1)
			for range partition.CaughtReplicas {
				result += LenReplicaId
			}
			result += varintSize(len(partition.OfflineReplicas) + 1)
			for range partition.OfflineReplicas {
				result += LenReplicaId
			}
			result += LenTaggedField
		}
		result += LenTopicAuthOperation + LenTaggedField
	}
	return result + LenClusterAuthOperation + LenTaggedField
}

func (m *MetadataResp) Bytes(version int16) []byte {
	bytes := make([]byte, m.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, m.CorrelationId)
	idx = putTaggedField(bytes, idx)
	idx = putThrottleTime(bytes, idx, 0)
	bytes[idx] = byte(len(m.BrokerMetadataList) + 1)
	idx++
	for _, brokerMetadata := range m.BrokerMetadataList {
		idx = putBrokerNodeId(bytes, idx, brokerMetadata.NodeId)
		idx = putHost(bytes, idx, brokerMetadata.Host)
		idx = putBrokerPort(bytes, idx, brokerMetadata.Port)
		idx = putRack(bytes, idx, brokerMetadata.Rack)
		idx = putTaggedField(bytes, idx)
	}
	idx = putClusterId(bytes, idx, m.ClusterId)
	idx = putInt(bytes, idx, m.ControllerId)
	bytes[idx] = byte(len(m.TopicMetadataList) + 1)
	idx++
	for _, topicMetadata := range m.TopicMetadataList {
		idx = putErrorCode(bytes, idx, topicMetadata.ErrorCode)
		idx = putTopic(bytes, idx, topicMetadata.Topic)
		idx = putBool(bytes, idx, topicMetadata.IsInternal)
		bytes[idx] = byte(len(topicMetadata.PartitionMetadataList) + 1)
		idx++
		for _, partitionMetadata := range topicMetadata.PartitionMetadataList {
			idx = putErrorCode(bytes, idx, partitionMetadata.ErrorCode)
			idx = putPartitionId(bytes, idx, partitionMetadata.PartitionId)
			idx = putLeaderId(bytes, idx, partitionMetadata.LeaderId)
			idx = putLeaderEpoch(bytes, idx, partitionMetadata.LeaderEpoch)
			bytes[idx] = byte(len(partitionMetadata.Replicas) + 1)
			idx++
			for _, replica := range partitionMetadata.Replicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			bytes[idx] = byte(len(partitionMetadata.CaughtReplicas) + 1)
			idx++
			for _, replica := range partitionMetadata.CaughtReplicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			bytes[idx] = byte(len(partitionMetadata.OfflineReplicas) + 1)
			idx++
			for _, replica := range partitionMetadata.OfflineReplicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			idx = putTaggedField(bytes, idx)
		}
		idx = putReplicaId(bytes, idx, topicMetadata.TopicAuthorizedOperation)
		idx = putTaggedField(bytes, idx)
	}
	idx = putClusterAuthorizedOperation(bytes, idx, m.ClusterAuthorizedOperation)
	idx = putTaggedField(bytes, idx)
	return bytes
}

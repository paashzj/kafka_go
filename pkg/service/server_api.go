package service

import (
	"net"
)

type KfkServer interface {
	// FetchPartition method called this already authed
	FetchPartition(addr *net.Addr, topic string, req *FetchPartitionReq) (*FetchPartitionResp, error)

	// GroupJoin method called this already authed
	GroupJoin(addr *net.Addr, req *JoinGroupReq) (*JoinGroupResp, error)

	// GroupLeave method called this already authed
	GroupLeave(addr *net.Addr, req *LeaveGroupReq) (*LeaveGroupResp, error)

	// GroupSync method called this already authed
	GroupSync(addr *net.Addr, req *SyncGroupReq) (*SyncGroupResp, error)

	// OffsetListPartition method called this already authed
	OffsetListPartition(addr *net.Addr, topic string, req *ListOffsetsPartitionReq) (*ListOffsetsPartitionResp, error)

	// OffsetCommitPartition method called this already authed
	OffsetCommitPartition(addr *net.Addr, topic string, req *OffsetCommitPartitionReq) (*OffsetCommitPartitionResp, error)

	// OffsetFetch method called this already authed
	OffsetFetch(addr *net.Addr, topic string, partition int) (*OffsetFetchPartitionResp, error)

	// Produce method called this already authed
	Produce(addr *net.Addr, topic string, partition int, req *ProducePartitionReq) (*ProducePartitionResp, error)

	SaslAuth(req SaslReq) (bool, ErrorCode)

	SaslAuthTopic(req SaslReq, topic string) (bool, ErrorCode)

	SaslAuthConsumerGroup(req SaslReq, consumerGroup string) (bool, ErrorCode)

	Disconnect(addr *net.Addr)
}

package service

import (
	"net"
)

type OffsetFetchReq struct {
	GroupId      string
	TopicReqList []*OffsetFetchTopicReq
}

type OffsetFetchTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetFetchPartitionReq
}

type OffsetFetchPartitionReq struct {
	PartitionId int
}

type OffsetFetchResp struct {
	ErrorCode     int16
	TopicRespList []*OffsetFetchTopicResp
}

type OffsetFetchTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetFetchPartitionResp
}

type OffsetFetchPartitionResp struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

func OffsetFetch(addr *net.Addr, impl KfkServer, req *OffsetFetchReq) (*OffsetFetchResp, error) {
	reqList := req.TopicReqList
	result := &OffsetFetchResp{}
	result.TopicRespList = make([]*OffsetFetchTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &OffsetFetchTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*OffsetFetchPartitionResp, 0)
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, _ := impl.OffsetFetch(addr, topicReq.Topic, partitionReq.PartitionId)
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		result.TopicRespList[i] = f
	}
	return result, nil
}

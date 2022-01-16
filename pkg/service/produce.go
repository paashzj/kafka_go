package service

import "net"

type ProduceReq struct {
	GroupId      string
	TopicReqList []*ProduceTopicReq
}

type ProduceTopicReq struct {
	Topic            string
	PartitionReqList []*ProducePartitionReq
}

type ProducePartitionReq struct {
	PartitionId int
	RecordBatch *RecordBatch
}

type ProduceResp struct {
	ErrorCode     int16
	TopicRespList []*ProduceTopicResp
}

type ProduceTopicResp struct {
	Topic             string
	PartitionRespList []*ProducePartitionResp
}

type ProducePartitionResp struct {
	PartitionId    int
	ErrorCode      int16
	Offset         int64
	Time           int64
	LogStartOffset int64
}

func Produce(addr *net.Addr, impl KfkServer, req *ProduceReq) (*ProduceResp, error) {
	reqList := req.TopicReqList
	result := &ProduceResp{}
	result.TopicRespList = make([]*ProduceTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &ProduceTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*ProducePartitionResp, 0)
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, _ := impl.Produce(addr, topicReq.Topic, partitionReq.PartitionId, partitionReq)
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		result.TopicRespList[i] = f
	}
	return result, nil
}

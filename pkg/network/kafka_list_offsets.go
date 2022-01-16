package network

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/paashzj/kafka_go/pkg/network/context"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

func (s *Server) ListOffsets(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 1 || version == 5 {
		return s.ListOffsetsVersion(ctx, frame, version)
	}
	logrus.Error("unknown offset version ", version)
	return nil, gnet.Close
}

func (s *Server) ListOffsetsVersion(ctx *context.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	req, err := codec.DecodeListOffsetReq(frame, version)
	if err != nil {
		return nil, gnet.Close
	}
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Info("list offset req ", req)
	lowOffsetReqList := make([]*service.ListOffsetsTopicReq, len(req.OffsetTopics))
	for i, topicReq := range req.OffsetTopics {
		if !s.checkSaslTopic(ctx, topicReq.Topic) {
			return nil, gnet.Close
		}
		lowTopicReq := &service.ListOffsetsTopicReq{}
		lowTopicReq.Topic = topicReq.Topic
		lowTopicReq.OffsetPartitionReqList = make([]*service.ListOffsetsPartitionReq, len(topicReq.ListOffsetPartitions))
		for j, partitionReq := range topicReq.ListOffsetPartitions {
			lowPartitionReq := &service.ListOffsetsPartitionReq{}
			lowPartitionReq.PartitionId = partitionReq.PartitionId
			lowTopicReq.OffsetPartitionReqList[j] = lowPartitionReq
		}
		lowOffsetReqList[i] = lowTopicReq
	}
	lowOffsetRespList, err := service.Offset(ctx.Addr, s.kafkaImpl, lowOffsetReqList)
	if err != nil {
		return nil, gnet.Close
	}
	resp := codec.NewListOffsetResp(req.CorrelationId)
	resp.OffsetTopics = make([]*codec.ListOffsetTopicResp, len(lowOffsetRespList))
	for i, lowTopicResp := range lowOffsetRespList {
		f := &codec.ListOffsetTopicResp{}
		f.Topic = lowTopicResp.Topic
		f.ListOffsetPartitions = make([]*codec.ListOffsetPartitionResp, len(lowTopicResp.OffsetPartitionRespList))
		for j, p := range lowTopicResp.OffsetPartitionRespList {
			partitionResp := &codec.ListOffsetPartitionResp{}
			partitionResp.PartitionId = p.PartitionId
			partitionResp.ErrorCode = 0
			partitionResp.Timestamp = p.Time
			partitionResp.Offset = p.Offset
			partitionResp.LeaderEpoch = 0
			f.ListOffsetPartitions[j] = partitionResp
		}
		resp.OffsetTopics[i] = f
	}
	return resp.Bytes(version), gnet.None
}

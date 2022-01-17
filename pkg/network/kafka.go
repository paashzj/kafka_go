package network

import (
	"encoding/binary"
	"fmt"
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/paashzj/kafka_go/pkg/codec/api"
	"github.com/paashzj/kafka_go/pkg/network/context"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
)

// connCount kafka connection count
var connCount int32

var connMutex sync.Mutex

type Config struct {
	ListenHost string
	ListenPort int
	MultiCore  bool
}

func Run(config *Config, kfkProtocolConfig *codec.KafkaProtocolConfig, impl service.KfkServer) (*Server, error) {
	server := &Server{
		EventServer:         nil,
		kafkaProtocolConfig: kfkProtocolConfig,
		kafkaImpl:           impl,
	}
	encoderConfig := gnet.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}
	decoderConfig := gnet.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
	kfkCodec := gnet.NewLengthFieldBasedFrameCodec(encoderConfig, decoderConfig)
	go func() {
		err := gnet.Serve(server, fmt.Sprintf("tcp://%s:%d", config.ListenHost, config.ListenPort), gnet.WithMulticore(config.MultiCore), gnet.WithCodec(kfkCodec))
		logrus.Error("kafsar broker started error ", err)
	}()
	return server, nil
}

type Server struct {
	*gnet.EventServer
	ConnMap             sync.Map
	SaslMap             sync.Map
	kafkaProtocolConfig *codec.KafkaProtocolConfig
	kafkaImpl           service.KfkServer
}

func (s *Server) OnInitComplete(server gnet.Server) (action gnet.Action) {
	logrus.Info("Kafka Server started")
	return
}

// React Kafka 协议格式为APIKey和API Version
// APIKey 样例: 00 12
func (s *Server) React(frame []byte, c gnet.Conn) ([]byte, gnet.Action) {
	logrus.Info("frame len is ", len(frame))
	if len(frame) < 5 {
		logrus.Error("invalid data packet")
		return nil, gnet.Close
	}
	connMutex.Lock()
	ctx := c.Context()
	if ctx == nil {
		addr := c.RemoteAddr()
		c.SetContext(&context.NetworkContext{Addr: &addr})
	}
	connMutex.Unlock()
	ctx = c.Context()
	networkContext := ctx.(*context.NetworkContext)
	apiKey := api.Code(binary.BigEndian.Uint16(frame))
	apiVersion := int16(binary.BigEndian.Uint16(frame[2:]))
	if apiKey == api.ApiVersions {
		return s.ApiVersions(frame[4:], apiVersion)
	}
	if apiKey == api.SaslHandshake {
		return s.SaslHandshake(frame[4:], apiVersion)
	}
	if apiKey == api.SaslAuthenticate {
		return s.SaslAuthenticate(frame[4:], apiVersion, networkContext)
	}
	if apiKey == api.Heartbeat {
		return s.Heartbeat(frame[4:], apiVersion)
	}
	if apiKey == api.JoinGroup {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.JoinGroup(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.SyncGroup {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.SyncGroup(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.OffsetFetch {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.OffsetFetch(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.ListOffsets {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.ListOffsets(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.Fetch {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.Fetch(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.OffsetCommit {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.OffsetCommit(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.LeaveGroup {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.LeaveGroup(networkContext, frame[4:], apiVersion)
	}
	if apiKey == api.Produce {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.Produce(networkContext, frame[4:], apiVersion, s.kafkaProtocolConfig)
	}
	if apiKey == api.Metadata {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.Metadata(frame[4:], apiVersion, s.kafkaProtocolConfig)
	}
	if apiKey == api.FindCoordinator {
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.FindCoordinator(frame[4:], apiVersion, s.kafkaProtocolConfig)
	}
	logrus.Error("unknown api ", apiKey, apiVersion)
	return nil, gnet.Close
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	if atomic.LoadInt32(&connCount) > s.kafkaProtocolConfig.MaxConn {
		logrus.Error("connection reach max, refused to connect ", c.RemoteAddr())
		return nil, gnet.Close
	}
	connCount := atomic.AddInt32(&connCount, 1)
	s.ConnMap.Store(c.RemoteAddr(), c)
	logrus.Info("new connection connected ", connCount, " from ", c.RemoteAddr())
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	logrus.Info("connection closed from ", c.RemoteAddr())
	s.ConnMap.Delete(c.RemoteAddr())
	s.SaslMap.Delete(c.RemoteAddr())
	atomic.AddInt32(&connCount, -1)
	return
}

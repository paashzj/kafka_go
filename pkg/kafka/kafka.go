package kafka

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/paashzj/kafka_go/pkg/network"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/sirupsen/logrus"
)

type ServerConfig struct {
	// 网络配置
	ListenAddr string
	MultiCore  bool
	NeedSasl   bool
	MaxConn    int32

	// Kafka协议配置
	ClusterId     string
	AdvertiseHost string
	AdvertisePort int
}

func Run(config *ServerConfig, impl service.KfkServer) (*ServerControl, error) {
	logrus.Info("This is codec message, you will see the message of codec")
	logrus.Info("This is network message， you will see the message of network")
	networkConfig := &network.Config{}
	networkConfig.ListenAddr = config.ListenAddr
	networkConfig.MultiCore = config.MultiCore
	kfkProtocolConfig := &codec.KafkaProtocolConfig{}
	kfkProtocolConfig.ClusterId = config.ClusterId
	kfkProtocolConfig.AdvertiseHost = config.AdvertiseHost
	kfkProtocolConfig.AdvertisePort = config.AdvertisePort
	kfkProtocolConfig.NeedSasl = config.NeedSasl
	kfkProtocolConfig.MaxConn = config.MaxConn
	serverControl := &ServerControl{}
	var err error
	serverControl.networkServer, err = network.Run(networkConfig, kfkProtocolConfig, impl)
	return serverControl, err
}

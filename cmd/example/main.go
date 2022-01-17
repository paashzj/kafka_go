package main

import (
	"flag"
	"github.com/paashzj/kafka_go/pkg/kafka"
	"github.com/sirupsen/logrus"
)

var listenAddr = flag.String("listen_host", "0.0.0.0", "kafka listen host")
var listenPort = flag.Int("listen_port", 9092, "kafka listen port")
var multiCore = flag.Bool("multi_core", false, "multi core")
var needSasl = flag.Bool("need_sasl", false, "need sasl")
var maxConn = flag.Int("max_conn", 500, "need sasl")

var clusterId = flag.String("cluster_id", "shoothzj", "kafka cluster id")
var advertiseListenAddr = flag.String("advertise_addr", "localhost", "kafka advertise addr")
var advertiseListenPort = flag.Int("advertise_port", 9092, "kafka advertise port")

func main() {
	flag.Parse()
	serverConfig := &kafka.ServerConfig{}
	serverConfig.ListenHost = *listenAddr
	serverConfig.ListenPort = *listenPort
	serverConfig.MultiCore = *multiCore
	serverConfig.NeedSasl = *needSasl
	serverConfig.ClusterId = *clusterId
	serverConfig.AdvertiseHost = *advertiseListenAddr
	serverConfig.AdvertisePort = *advertiseListenPort
	serverConfig.MaxConn = int32(*maxConn)
	e := &ExampleKafkaImpl{}
	_, err := kafka.Run(serverConfig, e)
	if err != nil {
		logrus.Error(err)
	}
}

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

package kafka

import (
	"github.com/paashzj/kafka_go/pkg/codec"
	"github.com/paashzj/kafka_go/pkg/network"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/sirupsen/logrus"
)

type ServerConfig struct {
	// network config
	ListenHost string
	ListenPort int
	MultiCore  bool
	NeedSasl   bool
	MaxConn    int32

	// Kafka protocol config
	ClusterId     string
	AdvertiseHost string
	AdvertisePort int
}

func Run(config *ServerConfig, impl service.KfkServer) (*ServerControl, error) {
	logrus.Info("This is codec message, you will see the message of codec")
	logrus.Info("This is network message， you will see the message of network")
	networkConfig := &network.Config{}
	networkConfig.ListenHost = config.ListenHost
	networkConfig.ListenPort = config.ListenPort
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

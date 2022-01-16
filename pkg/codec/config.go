package codec

type KafkaProtocolConfig struct {
	ClusterId     string
	NodeId        int32
	AdvertiseHost string
	AdvertisePort int
	NeedSasl      bool
	MaxConn       int32
}

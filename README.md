# kafka_go
[![codecov](https://codecov.io/gh/paashzj/kafka_go/branch/main/graph/badge.svg?token=155QKNN7MQ)](https://codecov.io/gh/paashzj/kafka_go)
## 其他语言文档
- [English Doc](README_en.md)
## 参考文档
### kafka协议文档
https://kafka.apache.org/protocol
# 服务端参数
## 通用配置
### LogLevel 日志级别
从1到10，10代表最详细的日志，5为默认级别。
- level7 网络编解码的信息
- level8 网络包的信息
## 网络配置
### ListenAddr
Kafka server的监听地址
### MultiCore 
是否多核
### MaxConn
能支撑的客户端最大连接数
## Kafka协议相关
### ClusterId 
Kafka集群Id
### AdvertiseHost
建议客户端连接的地址
### AdvertisePort
建议客户端连接的端口
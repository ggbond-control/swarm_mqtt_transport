# Swarm MQTT 传输

节点 `swarm_mqtt_transport_node` 转发设备协同消息及平台入口，不直接修改任务状态或操作 TaskHub。默认连接本机 Broker 和独立开发平台主题。

内部主题按集群、接收者和发送者隔离：`<mqtt_prefix>/<cluster>/broadcast/<sender>`、`.../to/<recipient>/<sender>`。QoS 1 非保留；拒绝 retained 控制和主题发送者不匹配的消息。Broker 必须设置每台设备的发布 ACL；主题校验不是数字签名。

控制与图片使用独立 MQTT 连接、主题和有界队列；图片不进入共识日志。控制优先，断连不缓存临时许可，业务 outbox 与 Raft 负责重传。网络中断时不能靠传输层绕过固定多数派。

远程 Broker 默认要求 TLS，只有显式开发选项才允许非加密远程连接。不要把真实密码写进版本化配置。配置、协议和运行边界见 [协调器 README](../swarm_mission_coordinator/README.md) 和 [PROTOCOL](../swarm_mission_coordinator/PROTOCOL.md)，测试见 [TEST_LOG](TEST_LOG.md)。

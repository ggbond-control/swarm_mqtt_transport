# swarm_mqtt_transport 测试记录

## 2026-11-17

验证固定平台与设备心跳新主题、不发布旧 fleet 心跳，以及远程 MQTT 无额外许可参数、
TLS 开启时两条连接均继续校验证书。真实 ROS／MQTT 集成及 CYVET 双开发板新主题观察、
隔离空任务闭环结果见 [Swarm TEST_LOG](../swarm_mission_coordinator/TEST_LOG.md#2026-11-17)。

## 2026-11-16

本包的单元测试位于 `test/`，跨包流程和故障验证统一归档于
[Swarm TEST_LOG](../swarm_mission_coordinator/TEST_LOG.md)，避免维护多份相互冲突的计数。
测试仅使用本地隔离环境，不连接开发板或生产 MQTT／MinIO。

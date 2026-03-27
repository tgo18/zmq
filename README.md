# ZMQ — Message Queue Middleware (Java 21)

从零实现的消息队列中间件，灵感来自 Apache Kafka，采用 Java 21 全新特性构建。

## 特性

- **单 Broker 架构**：单节点部署，适合学习和中小规模场景
- **主题与分区**：每个主题支持多分区，按消息 Key 做 Murmur2 哈希路由
- **消费者组**：多个消费者通过 `groupId` 共享偏移量，支持多播与独占消费
- **至少一次投递**：消费者显式 ACK 后才推进偏移量；崩溃重启后从上次提交点重新消费
- **持久化存储**：Append-Only 日志段（`.log` + `.index`），CRC32 校验，段滚动
- **长轮询 FETCH**：无新消息时服务端挂起最多 `maxWaitMs` 毫秒，有新消息立即唤醒
- **Java 21 特性**：虚拟线程、Records、Sealed Interfaces、Pattern Matching Switch

## 快速开始

详见 [使用手册](docs/user-guide.md)。

```bash
# 编译
javac --enable-preview --release 21 -cp "jackson/*.jar" \
  -d target/classes $(find src/main/java -name "*.java")

# 启动 Broker
java --enable-preview -cp "target/classes:jackson/*.jar" \
  com.zmq.Main --port 9092 --data ./data --partitions 4
```

## 文档

| 文档 | 说明 |
|------|------|
| [架构文档](docs/architecture.md) | 系统设计、模块结构、数据流 |
| [接口文档](docs/api-reference.md) | 客户端 SDK API、协议帧格式、命令规范 |
| [使用手册](docs/user-guide.md) | 编译运行、生产消费示例、配置说明 |

## 项目结构

```
src/main/java/com/zmq/
├── Main.java                     # 启动入口
├── broker/
│   ├── Broker.java               # Broker 主体（接收连接、调度）
│   ├── BrokerConfig.java         # 配置记录
│   ├── ConnectionHandler.java    # 每连接处理器（虚拟线程）
│   ├── TopicManager.java         # 主题/分区/路由管理
│   └── SubscriptionManager.java  # 消费组偏移量管理
├── client/
│   ├── MQClient.java             # TCP 连接 + 异步响应关联
│   ├── Producer.java             # 生产者 SDK
│   └── Consumer.java             # 消费者 SDK
├── message/
│   ├── Message.java              # 消息记录
│   └── Topic.java                # 主题元数据记录
├── protocol/
│   ├── Frame.java                # 帧格式常量
│   ├── Command.java              # 协议命令（Sealed Interface）
│   ├── CommandEncoder.java       # 编码：Command → 二进制帧
│   └── CommandParser.java        # 解码：二进制帧 → Command
└── storage/
    ├── LogSegment.java           # 日志段文件（.log）
    ├── MessageLog.java           # 日志段管理器
    └── OffsetIndex.java          # 稀疏偏移索引（.index）
```

## 测试

38 个测试全部通过：

| 测试类 | 数量 | 覆盖内容 |
|--------|------|---------|
| `ProtocolTest` | 15 | 所有命令类型的帧编解码往返 |
| `MessageLogTest` | 8 | 存储层追加、读取、崩溃恢复、段滚动、保留策略 |
| `TopicManagerTest` | 7 | 主题生命周期、路由、元数据持久化 |
| `BrokerIntegrationTest` | 8 | 端到端：发布消费、至少一次投递、并发生产者 |

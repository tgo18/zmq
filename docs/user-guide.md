# ZMQ 使用手册

## 1. 环境要求

| 依赖 | 版本 | 说明 |
|------|------|------|
| JDK | 21+ | 必须支持虚拟线程和 Preview Features |
| Maven | 3.8+ | 可选；也可直接用 javac 编译 |
| Jackson | 2.16.x | JSON 序列化（`jackson-databind`, `jackson-core`, `jackson-annotations`, `jackson-datatype-jdk8`）|

---

## 2. 编译与打包

### 2.1 使用 Maven（推荐）

```bash
# 编译 + 测试
mvn clean test

# 打包成可执行 Fat JAR（包含所有依赖）
mvn clean package -DskipTests

# 生成 target/zmq-1.0.0.jar
```

### 2.2 直接使用 javac

```bash
# 将 Jackson JAR 放到 lib/ 目录
LIB=lib
CP="$LIB/jackson-databind-2.16.1.jar:$LIB/jackson-core-2.16.1.jar:\
$LIB/jackson-annotations-2.16.1.jar:$LIB/jackson-datatype-jdk8-2.16.1.jar:\
$LIB/slf4j-api-2.0.12.jar:$LIB/slf4j-simple-2.0.12.jar"

# 编译
find src/main/java -name "*.java" | xargs \
  javac --enable-preview --release 21 -cp "$CP" -d target/classes

# 编译测试
JUNIT_CP="$LIB/junit-jupiter-api-5.10.2.jar:$LIB/junit-platform-console-1.10.2.jar:..."
find src/test/java -name "*.java" | xargs \
  javac --enable-preview --release 21 -cp "$CP:target/classes:$JUNIT_CP" -d target/test-classes
```

---

## 3. 启动 Broker

### 3.1 使用 Fat JAR

```bash
java --enable-preview -jar target/zmq-1.0.0.jar \
  --port 9092 \
  --data /var/lib/zmq/data \
  --partitions 4
```

### 3.2 使用 Main 类

```bash
java --enable-preview \
  -cp "target/classes:lib/*" \
  com.zmq.Main \
  --port 9092 \
  --data ./data \
  --partitions 4
```

### 3.3 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port <port>` | 9092 | 客户端连接监听端口 |
| `--admin-port <port>` | 9093 | 管理端口（保留）|
| `--data <dir>` | `./data` | 持久化数据目录（自动创建）|
| `--partitions <n>` | 4 | 自动创建主题时的默认分区数 |

### 3.4 启动日志示例

```
╔══════════════════════════════════════╗
║        ZMQ Message Queue             ║
║        Java 21 Edition               ║
╚══════════════════════════════════════╝
[main] INFO  com.zmq.Main - Starting broker on port 9092 | data dir: data
[main] INFO  com.zmq.broker.Broker - Starting ZMQ broker on port 9092...
[main] INFO  com.zmq.broker.Broker - ZMQ broker started. Listening on port 9092
```

### 3.5 优雅停止

Broker 注册了 JVM Shutdown Hook，收到 `SIGTERM` 或 `Ctrl+C` 时自动：
1. 刷盘所有日志段
2. 持久化消费组偏移量
3. 关闭所有连接

---

## 4. 快速开始示例

以下示例均需先启动 Broker。

### 4.1 发布消息

```java
import com.zmq.client.MQClient;
import com.zmq.client.Producer;

try (MQClient client = new MQClient("localhost", 9092)) {
    client.connect();
    Producer producer = client.createProducer();

    // 先创建主题（可选，不创建时 Broker 自动以 1 个分区创建）
    client.sendAndReceive(new com.zmq.protocol.Command.CreateTopic(
        "orders", 4, -1L, 7 * 24 * 60 * 60 * 1000L
    ));

    // 发布单条消息
    long offset = producer.send("orders", "订单内容".getBytes());
    System.out.println("发布成功，偏移量：" + offset);

    // 带路由 Key 发布（相同 key 路由到相同分区）
    long offset2 = producer.send("orders", "user-1001",
        "{\"orderId\":42,\"amount\":99.9}".getBytes(),
        Map.of("content-type", "application/json"));

    // 批量发布
    List<Long> offsets = producer.sendBatch("orders",
        List.of("msg1".getBytes(), "msg2".getBytes()));
}
```

### 4.2 消费消息（自动提交）

```java
import com.zmq.client.MQClient;
import com.zmq.client.Consumer;
import com.zmq.message.Message;

try (MQClient client = new MQClient("localhost", 9092)) {
    client.connect();
    Consumer consumer = client.createConsumer("my-group");
    consumer.subscribe("orders");  // 从上次提交的偏移量开始

    for (int i = 0; i < 10; i++) {
        List<Message> msgs = consumer.poll(1000);  // 最多等 1 秒
        for (Message msg : msgs) {
            System.out.printf("offset=%d, key=%s, payload=%s%n",
                msg.offset(), msg.key(), new String(msg.payload()));
        }
        if (!msgs.isEmpty()) {
            consumer.commitSync();  // 处理完毕后提交偏移量
        }
    }
}
```

### 4.3 至少一次投递（处理后再提交）

```java
Consumer consumer = client.createConsumer("billing");
consumer.subscribe("orders");

while (true) {
    List<Message> msgs = consumer.poll(500);
    if (msgs.isEmpty()) continue;

    try {
        for (Message msg : msgs) {
            processOrder(msg);     // 处理业务逻辑
        }
        consumer.commitSync();     // 全部处理成功后才提交
    } catch (Exception e) {
        // 处理失败：不提交 → 下次重启/重连仍从上次提交点消费
        log.error("处理失败，将重试", e);
    }
}
```

### 4.4 多个消费者组（广播模式）

```java
// 消费者组 A：billing 服务
Consumer billingConsumer = clientA.createConsumer("billing");
billingConsumer.subscribe("orders");

// 消费者组 B：analytics 服务（独立偏移量，消费相同的消息）
Consumer analyticsConsumer = clientB.createConsumer("analytics");
analyticsConsumer.subscribe("orders");
```

### 4.5 手动管理偏移量

```java
Consumer consumer = client.createConsumer("my-group");
consumer.subscribe("events");

// 跳转到指定偏移量（仅本地游标，不影响 Broker 侧提交值）
consumer.seek("events", 0, 100L);

// 精细化 ACK（逐条确认）
List<Message> msgs = consumer.poll(500);
for (Message msg : msgs) {
    if (processMessage(msg)) {
        consumer.ack(msg.topic(), msg.partition(), msg.offset());
    }
}
```

### 4.6 主题管理

```java
import com.zmq.protocol.Command;

// 创建主题（4 分区，7 天保留）
client.sendAndReceive(new Command.CreateTopic(
    "events", 4, -1L, 7 * 24 * 3600 * 1000L
));

// 列举所有主题
Command.ResponseOk resp = (Command.ResponseOk) client.sendAndReceive(new Command.ListTopics());
List<String> topics = (List<String>) resp.data().get("topics");

// 查询主题详情（分区偏移量）
Command.ResponseOk info = (Command.ResponseOk) client.sendAndReceive(
    new Command.TopicInfo("events")
);
Map<String, Long> partitionOffsets = (Map<String, Long>) info.data().get("partitionOffsets");

// 删除主题
client.sendAndReceive(new Command.DeleteTopic("events"));
```

---

## 5. 消费语义说明

### 5.1 偏移量规则

- 偏移量从 0 开始，每条消息递增 1，在同一分区内全局唯一
- 提交偏移量 N 表示：已成功处理 `offset ≤ N` 的所有消息
- 下次消费从 `N + 1` 开始

### 5.2 至少一次 vs 最多一次

| 模式 | 实现方式 | 风险 |
|------|---------|------|
| **至少一次**（推荐）| 处理完成后调用 `commitSync()` | 处理前崩溃 → 重复消费 |
| **最多一次** | 收到消息后立即 `commitSync()`，再处理 | 处理时崩溃 → 消息丢失 |

### 5.3 消费组重启后的行为

| 场景 | 消费起点 |
|------|---------|
| 首次消费（无历史提交）| 从偏移量 0 开始 |
| 有历史提交 | 从上次 `commitSync()` 的偏移量 + 1 开始 |
| 手动 `seek()` | 从 seek 指定的偏移量开始（仅影响本次运行，不持久化）|

---

## 6. 分区路由规则

```
partition = Murmur2(key.getBytes("UTF-8")) % numPartitions
```

- key 为 `null` 或空字符串 → 始终路由到分区 **0**
- 相同 key 保证路由到相同分区（消息有序性）
- 生产时也可指定 `partition >= 0` 直接写入指定分区（绕过路由）

---

## 7. 数据目录结构

```
data/
├── topics.json               # 主题元数据（Broker 重启后恢复主题）
├── offsets.json              # 消费组偏移量（Broker 重启后恢复偏移）
└── orders/                   # 主题 "orders"
    ├── 0/                    # 分区 0
    │   ├── 00000000000000000000.log    # 第一个段（基偏移量 0）
    │   └── 00000000000000000000.index  # 稀疏索引
    ├── 1/                    # 分区 1
    │   └── ...
    └── 2/                    # 分区 2
        └── ...
```

---

## 8. 性能与限制

| 项目 | 值 | 说明 |
|------|-----|------|
| 最大连接数 | 1000 | 可通过 `BrokerConfig` 调整 |
| 最大帧大小 | 64 MB | 单条消息（含 JSON 开销）不超过 64 MB |
| 段文件大小 | 256 MB | 超过后自动滚动，可配置 |
| 消息保留时间 | 7 天 | 可在创建主题时单独配置 |
| 索引粒度 | 每 4096 字节一个条目 | 固定，影响随机读性能 |

---

## 9. 运行测试

```bash
# Maven 方式
mvn test

# 直接运行（需要已编译的 test-classes）
java --enable-preview \
  -cp "target/classes:target/test-classes:lib/*" \
  org.junit.platform.console.ConsoleLauncher \
  --scan-classpath=target/test-classes \
  --include-engine=junit-jupiter
```

测试套件（38 个测试）：

| 测试类 | 测试方法 |
|--------|---------|
| `ProtocolTest` | testPublishRoundTrip, testSubscribeRoundTrip, testFetchRoundTrip, testAckRoundTrip, testCreateTopicRoundTrip, testDeleteTopicRoundTrip, testListTopicsRoundTrip, testTopicInfoRoundTrip, testPingPongRoundTrip, testResponseOkRoundTrip, testResponseErrorRoundTrip, testMessageBatchRoundTrip, testUnsubscribeRoundTrip, testInvalidMagicThrows, testMultipleFramesInSequence, testEmptyPayloadPublish |
| `MessageLogTest` | testAppendAndRead, testReadFromMiddle, testReadBeyondEndReturnsEmpty, testSegmentRolling, testCrashRecovery, testRetentionBySize, testLargeNumberOfMessages, testEmptyLogRead |
| `TopicManagerTest` | testCreateAndGetTopic, testIdempotentCreate, testPartitionRouting, testAppendAndRead, testDeleteTopic, testMetadataPersistence, testListTopics |
| `BrokerIntegrationTest` | testPublishAndConsume, testAtLeastOnceDelivery, testConsumerGroupOffset, testConcurrentProducers, testCreateAndDeleteTopic, testLongPollFetch, testTopicAutoCreate, testMultiPartition |

---

## 10. 常见问题

**Q: 消费者重启后收到了重复消息？**

A: 这是至少一次投递的正常行为。若上次消费后未调用 `commitSync()`，重启后会从上次提交点重新消费。确保业务处理具有幂等性，或每次处理后立即调用 `commitSync()`。

**Q: 发布消息时抛出 `IOException: Publish failed [PUBLISH_FAILED]`？**

A: 通常由以下原因导致：
- 连接已断开，需重新 `client.connect()`
- 消息体过大（超过 64 MB）
- Broker 内部存储错误（检查 Broker 日志）

**Q: 如何实现多消费者负载均衡？**

A: ZMQ 当前不支持消费者组内的分区再平衡（类似 Kafka 的 Consumer Group Rebalance）。变通方案：将不同分区手动分配给不同消费者实例：

```java
// 消费者实例 1 消费分区 0
consumer1.subscribe("orders", 0);

// 消费者实例 2 消费分区 1
consumer2.subscribe("orders", 1);
```

**Q: Broker 崩溃后数据会丢失吗？**

A: 消息写入 `.log` 文件后即持久化（操作系统页缓存）。Broker 定时调用 `flush()` 刷盘；关闭时强制刷盘。极端情况下（写入后未刷盘即崩溃）可能丢失最近几条消息。如需零丢失，可在发布后手动触发 flush（当前未暴露 flush 接口）。

消费组偏移量每次 `commitOffset` 后立即写入 `offsets.json`，Broker 重启后自动恢复。

**Q: 如何查看当前的消费积压（lag）？**

A: 通过 `TOPIC_INFO` 命令获取各分区的 `nextOffset`（生产端进度），减去消费组已提交的偏移量即为积压量。`SubscriptionManager.groupInfo()` 内部已实现此计算，可在自定义管理接口中使用。

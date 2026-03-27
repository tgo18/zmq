# ZMQ 接口文档（API Reference）

## 1. 客户端 SDK

### 1.1 MQClient

TCP 连接管理器，是 Producer 和 Consumer 的底层依赖。

```java
package com.zmq.client;

public class MQClient implements AutoCloseable {

    /** 创建客户端（尚未连接）*/
    public MQClient(String host, int port);

    /** 建立 TCP 连接并启动读循环（虚拟线程）*/
    public void connect() throws IOException;

    /** 发送一条命令并阻塞等待响应（虚拟线程安全）*/
    public Command sendAndReceive(Command command) throws IOException;

    /** 创建生产者（共享本连接）*/
    public Producer createProducer();

    /** 创建消费者（共享本连接，指定消费组）*/
    public Consumer createConsumer(String groupId);

    /** 关闭连接，挂起的 Future 会抛出 IOException */
    @Override
    public void close() throws IOException;
}
```

**使用示例：**

```java
try (MQClient client = new MQClient("localhost", 9092)) {
    client.connect();
    Producer producer = client.createProducer();
    Consumer consumer = client.createConsumer("my-group");
    // ...
}
```

---

### 1.2 Producer

高级发布 API，封装 `Command.Publish`。

```java
package com.zmq.client;

public class Producer implements AutoCloseable {

    /**
     * 发布消息到指定主题（分区自动路由）。
     * 若主题不存在，Broker 自动创建（1 个分区）。
     *
     * @param topic   目标主题名
     * @param payload 消息内容（字节数组）
     * @return        Broker 分配的逻辑偏移量
     * @throws IOException 发布失败（网络错误或 Broker 返回错误）
     */
    public long send(String topic, byte[] payload) throws IOException;

    /**
     * 带路由 Key 和自定义 Headers 的发布。
     * Key 用于 Murmur2 哈希路由；null/空 Key 路由到分区 0。
     *
     * @param topic   目标主题名
     * @param key     路由键（可为 null）
     * @param payload 消息内容
     * @param headers 自定义头部（String → String 映射）
     * @return        Broker 分配的逻辑偏移量
     */
    public long send(String topic, String key, byte[] payload,
                     Map<String, String> headers) throws IOException;

    /**
     * 批量发布（顺序逐条发送，返回每条消息的偏移量列表）。
     *
     * @param topic    目标主题名
     * @param messages 消息列表
     * @return         偏移量列表，顺序与入参一致
     */
    public List<Long> sendBatch(String topic, List<byte[]> messages) throws IOException;

    @Override
    public void close();  // 不关闭底层 MQClient
}
```

**使用示例：**

```java
Producer producer = client.createProducer();

// 简单发布
long offset = producer.send("orders", "hello".getBytes());

// 带 Key 的发布（保证相同 Key 路由到同一分区）
long offset2 = producer.send("orders", "user-123",
    "{\"amount\":100}".getBytes(), Map.of("content-type", "json"));

// 批量发布
List<Long> offsets = producer.sendBatch("orders",
    List.of("msg1".getBytes(), "msg2".getBytes(), "msg3".getBytes()));
```

---

### 1.3 Consumer

基于拉取模式的消费者，支持至少一次投递语义。

```java
package com.zmq.client;

public class Consumer implements AutoCloseable {

    /**
     * 订阅主题的分区 0。
     * Broker 将从该消费组的上次提交偏移量处继续投递。
     *
     * @throws IOException 主题不存在时抛出
     */
    public void subscribe(String topic) throws IOException;

    /**
     * 订阅主题的指定分区。
     *
     * @param topic     主题名
     * @param partition 分区号（0-based）
     */
    public void subscribe(String topic, int partition) throws IOException;

    /**
     * 拉取消息（长轮询）。
     * 若无新消息，阻塞最多 maxWaitMs 毫秒后返回空列表。
     * 遍历所有已订阅的 (topic, partition) 对。
     *
     * @param maxWaitMs 最长等待时间（毫秒）；0 表示立即返回
     * @return          收到的消息列表（可能为空）
     */
    public List<Message> poll(long maxWaitMs) throws IOException;

    /**
     * 将当前消费游标同步提交到 Broker。
     * 提交成功后，下次重启的消费者将从提交点之后开始消费。
     * 调用时机：确认消息已成功处理之后。
     *
     * @throws IOException 提交失败时抛出
     */
    public void commitSync() throws IOException;

    /**
     * 显式 ACK 单条消息（精细化控制）。
     * 提交 offset 后，本地游标也更新为 offset + 1。
     *
     * @param topic     主题名
     * @param partition 分区号
     * @param offset    已处理消息的逻辑偏移量
     */
    public void ack(String topic, int partition, long offset) throws IOException;

    /**
     * 手动定位到指定偏移量（下次 poll 从此处开始）。
     * 不影响 Broker 侧的已提交偏移量。
     *
     * @param topic     主题名
     * @param partition 分区号
     * @param offset    目标偏移量
     */
    public void seek(String topic, int partition, long offset);

    @Override
    public void close();  // 不关闭底层 MQClient
}
```

**使用示例：**

```java
Consumer consumer = client.createConsumer("billing");
consumer.subscribe("orders");

while (true) {
    List<Message> msgs = consumer.poll(1000);  // 最多等 1 秒
    for (Message msg : msgs) {
        process(msg);
    }
    if (!msgs.isEmpty()) {
        consumer.commitSync();  // 处理完毕后提交
    }
}
```

---

### 1.4 Message（消息记录）

```java
package com.zmq.message;

public record Message(
    String id,                      // 消息唯一 ID（UUID）
    String topic,                   // 所属主题
    int partition,                  // 所在分区（0-based）
    long offset,                    // 逻辑偏移量（-1 表示未分配）
    byte[] payload,                 // 消息内容
    Map<String, String> headers,    // 自定义头部
    Instant timestamp,              // 生产时间戳（Broker 侧赋值）
    String key                      // 路由键（可为 null）
) {
    /** 创建待发布消息（offset=-1，timestamp=now）*/
    public static Message create(String id, String topic, int partition,
                                 byte[] payload, Map<String, String> headers, String key);

    /** 返回带指定偏移量的消息副本（不可变记录，返回新实例）*/
    public Message withOffset(long offset);
}
```

---

## 2. 协议命令（Command）详细规范

所有命令均为 `Command` sealed interface 的 record 子类，通过 JSON 负载传输。

### 2.1 PUBLISH（0x01） — 客户端→Broker

**请求 JSON：**

```json
{
  "topic":            "orders",
  "partition":        -1,
  "key":              "user-123",
  "payload":          "<base64编码的消息内容>",
  "headers":          { "content-type": "json" },
  "produceTimeoutMs": 5000
}
```

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| topic | string | ✓ | — | 目标主题 |
| partition | int | — | -1 | -1 表示按 key 自动路由 |
| key | string | — | null | 路由键，null/空时路由到分区 0 |
| payload | string | — | "" | Base64 编码的消息内容 |
| headers | object | — | {} | 字符串键值对 |
| produceTimeoutMs | long | — | 5000 | 生产超时（毫秒） |

**成功响应（RESPONSE_OK）：**

```json
{
  "topic":     "orders",
  "partition": 0,
  "offset":    42
}
```

**失败响应（RESPONSE_ERROR）：**

```json
{
  "errorCode": "PUBLISH_FAILED",
  "message":   "错误详情"
}
```

---

### 2.2 SUBSCRIBE（0x02） — 客户端→Broker

**请求 JSON：**

```json
{
  "topic":     "orders",
  "partition": 0,
  "groupId":   "billing"
}
```

**成功响应（RESPONSE_OK）：**

```json
{
  "topic":   "orders",
  "groupId": "billing"
}
```

**失败响应：** `TOPIC_NOT_FOUND`

---

### 2.3 FETCH（0x04） — 客户端→Broker

**请求 JSON：**

```json
{
  "topic":       "orders",
  "partition":   0,
  "groupId":     "billing",
  "fromOffset":  -1,
  "maxMessages": 100,
  "maxWaitMs":   1000
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| fromOffset | long | -1 | -1 表示从 groupId 的已提交偏移量+1 开始 |
| maxMessages | int | 100 | 单次最多返回消息数 |
| maxWaitMs | long | 0 | 无消息时最长等待毫秒数；0=立即返回 |

**成功响应（MESSAGE_BATCH，0x0E）：**

```json
{
  "topic":     "orders",
  "partition": 0,
  "messages": [
    {
      "id":        "550e8400-e29b-41d4-a716-446655440000",
      "topic":     "orders",
      "partition": 0,
      "offset":    42,
      "payload":   "<base64>",
      "headers":   {},
      "timestamp": 1711526400000,
      "key":       "user-123"
    }
  ]
}
```

**失败响应：** `TOPIC_NOT_FOUND`、`FETCH_FAILED`

---

### 2.4 ACK（0x05） — 客户端→Broker

**请求 JSON：**

```json
{
  "topic":     "orders",
  "partition": 0,
  "groupId":   "billing",
  "offset":    42
}
```

提交 offset 含义：已成功处理偏移量 ≤ offset 的所有消息；下次从 offset+1 开始消费。

**成功响应（RESPONSE_OK）：**

```json
{
  "topic":     "orders",
  "partition": 0,
  "groupId":   "billing",
  "offset":    42
}
```

**失败响应：** `TOPIC_NOT_FOUND`、`ACK_FAILED`

---

### 2.5 CREATE_TOPIC（0x06） — 客户端→Broker

**请求 JSON：**

```json
{
  "name":          "orders",
  "numPartitions": 4,
  "retentionBytes": -1,
  "retentionMs":   604800000
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| numPartitions | int | 1 | 分区数量（≥1）|
| retentionBytes | long | -1 | 每个分区的最大字节数；-1 不限制 |
| retentionMs | long | 604800000 | 消息最长保留时间（毫秒）；默认 7 天 |

创建已存在的主题为幂等操作（返回成功，不修改已有配置）。

**成功响应：** `{ "name": "orders", "numPartitions": 4 }`
**失败响应：** `CREATE_TOPIC_FAILED`

---

### 2.6 DELETE_TOPIC（0x07）

**请求 JSON：** `{ "name": "orders" }`
**成功响应：** `{ "name": "orders" }`
**失败响应：** `TOPIC_NOT_FOUND`、`DELETE_TOPIC_FAILED`

---

### 2.7 LIST_TOPICS（0x08）

**请求 JSON：** `{}`（空对象）
**成功响应：** `{ "topics": ["orders", "payments", "events"] }`

---

### 2.8 TOPIC_INFO（0x09）

**请求 JSON：** `{ "topicName": "orders" }`

**成功响应：**

```json
{
  "name":            "orders",
  "numPartitions":   4,
  "createdAt":       "2024-03-27T12:00:00Z",
  "partitionOffsets": {
    "0": 100,
    "1": 95,
    "2": 103,
    "3": 98
  }
}
```

**失败响应：** `TOPIC_NOT_FOUND`

---

### 2.9 PING / PONG（0x0A / 0x0B）

**PING 请求：** `{ "timestamp": 1711526400000 }`
**PONG 响应：** `{ "timestamp": 1711526400000 }`（回显相同时间戳）

---

### 2.10 通用响应格式

**RESPONSE_OK（0x0C）：**

```json
{
  "correlationId": "",
  "data": { ... }
}
```

**RESPONSE_ERROR（0x0D）：**

```json
{
  "correlationId": "",
  "errorCode": "TOPIC_NOT_FOUND",
  "message": "Topic not found: orders"
}
```

**已定义错误码：**

| 错误码 | 触发命令 | 说明 |
|--------|---------|------|
| `TOPIC_NOT_FOUND` | SUBSCRIBE/FETCH/ACK/DELETE_TOPIC/TOPIC_INFO | 主题不存在 |
| `PUBLISH_FAILED` | PUBLISH | 发布时发生内部错误 |
| `FETCH_FAILED` | FETCH | 拉取时发生内部错误 |
| `CREATE_TOPIC_FAILED` | CREATE_TOPIC | 创建主题失败 |
| `DELETE_TOPIC_FAILED` | DELETE_TOPIC | 删除主题失败 |
| `PROTOCOL_ERROR` | 任意 | 帧格式错误（MAGIC 不匹配等）|
| `INTERNAL_ERROR` | 任意 | 未分类内部错误 |

---

## 3. 存储 API（内部接口）

### 3.1 MessageLog

```java
package com.zmq.storage;

public class MessageLog implements AutoCloseable {

    /** 创建或加载指定目录下的日志（恢复已有段）*/
    public MessageLog(Path logDir, long maxSegmentBytes) throws IOException;

    /** 追加原始字节记录，返回分配的逻辑偏移量 */
    public long append(byte[] data) throws IOException;

    /**
     * 读取最多 maxCount 条记录，从 fromOffset 开始。
     * 自动跨段读取。若 fromOffset 超出日志末尾，返回空列表。
     */
    public List<byte[]> read(long fromOffset, int maxCount) throws IOException;

    /** 下一条追加记录将获得的偏移量 */
    public long nextOffset();

    /** 强制刷盘 */
    public void flush() throws IOException;

    /** 应用保留策略（时间维度 + 大小维度，活跃段不删除）*/
    public void applyRetention(Duration maxAge, long maxTotalBytes) throws IOException;

    @Override
    public void close() throws IOException;
}
```

### 3.2 LogSegment

```java
package com.zmq.storage;

public class LogSegment implements AutoCloseable {

    /** 打开或创建段文件（自动崩溃恢复：扫描末尾重建 nextOffset）*/
    public LogSegment(Path logPath, Path indexPath, long baseOffset) throws IOException;

    /** 追加记录，返回逻辑偏移量（线程安全：synchronized）*/
    public synchronized long append(byte[] data) throws IOException;

    /** 从物理位置 position 开始，顺序读取最多 maxCount 条记录 */
    public synchronized List<byte[]> readFrom(long position, int maxCount) throws IOException;

    /** 将逻辑偏移量转换为物理文件位置（借助稀疏索引加速）*/
    public synchronized long positionForOffset(long targetOffset) throws IOException;

    public long baseOffset();
    public long nextOffset();
    public long sizeBytes();

    public void flush() throws IOException;

    @Override
    public void close() throws IOException;
}
```

### 3.3 OffsetIndex

```java
package com.zmq.storage;

public class OffsetIndex implements AutoCloseable {

    /** 打开或创建索引文件 */
    public OffsetIndex(Path indexPath) throws IOException;

    /** 追加一条（offset, position）索引条目 */
    public synchronized void append(long offset, long position) throws IOException;

    /**
     * 查找 targetOffset 对应的最近物理位置（不超过 targetOffset 的最大索引条目）。
     * 若无匹配条目，返回 0。
     */
    public synchronized long lookup(long targetOffset) throws IOException;

    @Override
    public void close() throws IOException;
}
```

---

## 4. Broker 配置

`BrokerConfig` 是不可变记录，提供默认配置 `BrokerConfig.DEFAULT`。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `port` | int | 9092 | 客户端连接端口 |
| `adminPort` | int | 9093 | 管理端口（保留，暂未使用）|
| `dataDir` | Path | `./data` | 持久化数据目录 |
| `defaultPartitions` | int | 4 | 自动创建主题的默认分区数 |
| `maxSegmentBytes` | long | 268435456 (256MB) | 单个日志段最大字节数 |
| `retentionMs` | long | 604800000 (7天) | 默认消息保留时间（毫秒）|
| `retentionBytes` | long | -1 | 默认每分区最大字节数；-1 不限制 |
| `heartbeatIntervalMs` | int | 30000 | 心跳检测间隔（毫秒）|
| `heartbeatTimeoutMs` | int | 90000 | 连接超时阈值（毫秒）|
| `maxConnections` | int | 1000 | 最大并发连接数 |

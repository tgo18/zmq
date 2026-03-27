# ZMQ 架构文档

## 1. 整体架构

ZMQ 是一个单节点消息队列，采用三层平面设计：

```
┌─────────────────────────────────────────────────────────┐
│                    客户端进程                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────────┐ │
│  │ Producer │   │ Consumer │   │       MQClient       │ │
│  └────┬─────┘   └────┬─────┘   │  (TCP + Future 关联)  │ │
│       └──────────────┴─────────┴──────────────────────┘ │
│                        TCP / 9092                         │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                      Broker 进程                          │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              网络层 (Network Plane)                   │ │
│  │  ServerSocket → 每连接一个虚拟线程 → ConnectionHandler│ │
│  └──────────────────────┬──────────────────────────────┘ │
│                         │ Command（Sealed Interface）     │
│  ┌──────────────────────▼──────────────────────────────┐ │
│  │              控制层 (Control Plane)                   │ │
│  │  TopicManager          SubscriptionManager           │ │
│  │  (主题/分区/路由)        (消费组偏移量)                 │ │
│  └──────────────────────┬──────────────────────────────┘ │
│                         │                                 │
│  ┌──────────────────────▼──────────────────────────────┐ │
│  │               数据层 (Data Plane)                    │ │
│  │  MessageLog → LogSegment (.log)  + OffsetIndex (.index)│ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  持久化文件：                                              │
│    data/<topic>/<partition>/00000...0.log                 │
│    data/<topic>/<partition>/00000...0.index               │
│    data/topics.json                                       │
│    data/offsets.json                                      │
└───────────────────────────────────────────────────────────┘
```

---

## 2. 模块说明

### 2.1 网络层

**`Broker`** — 服务器主体
- 绑定 `ServerSocket`，在虚拟线程上运行 accept 循环
- 每个客户端连接分配一个独立虚拟线程（`Thread.ofVirtual()`），运行 `ConnectionHandler`
- 使用 `ScheduledExecutorService` 定期执行日志保留清理和心跳检测

**`ConnectionHandler`** — 单连接生命周期
- 循环读取命令帧 → 分发处理 → 写回响应
- 使用 Java 21 pattern matching switch 进行完备的命令分发：

```java
Command dispatch(Command cmd) {
    return switch (cmd) {
        case Command.Publish p      -> handlePublish(p);
        case Command.Fetch f        -> handleFetch(f);
        case Command.Ack a          -> handleAck(a);
        // ... 全部 14 个分支，编译器强制完备性
    };
}
```

**`MQClient`** — 客户端 TCP 连接管理器
- 启动一个虚拟线程读循环，异步接收响应
- 每次 `sendAndReceive` 分配递增的 requestId，存入 `ConcurrentHashMap<Long, CompletableFuture<Command>>`
- 读循环按 FIFO 顺序完成 Future（单连接单请求流）

---

### 2.2 控制层

**`TopicManager`**
- 维护 `ConcurrentHashMap<String, TopicState>`（topic name → 元数据 + 分区日志）
- 分区路由：Murmur2 哈希（与 Kafka 默认分区器兼容）
- 长轮询通知：`ConcurrentHashMap<String, CountDownLatch>` 键为 `"topic:partition"`；
  追加时调用 `notifyFetchWaiters` 释放等待的 FETCH

**`SubscriptionManager`**
- 三层嵌套 `ConcurrentHashMap`：`groupId → topic → partition → committedOffset`
- `commitOffset` 更新内存 + 异步写入 `offsets.json`
- Broker 启动时调用 `loadOffsets()` 恢复崩溃前状态

---

### 2.3 数据层

#### 日志段（LogSegment）

每个分区由一组按基偏移量命名的段文件组成：

```
00000000000000000000.log    ← 基偏移量 = 0
00000000000000000000.index
00000000000000001024.log    ← 基偏移量 = 1024（段滚动后）
00000000000000001024.index
```

**.log 文件记录格式**（二进制，每条记录紧密追加）：

```
┌──────────────────────────────────────┐
│  4 bytes  │  record length (int)     │
│  4 bytes  │  CRC32 checksum (int)    │
│  N bytes  │  JSON message payload    │
└──────────────────────────────────────┘
```

**.index 文件条目格式**（稀疏索引，每 4096 字节写一条）：

```
┌──────────────────────────────────────┐
│  8 bytes  │  logical offset (long)   │
│  8 bytes  │  physical position (long)│
└──────────────────────────────────────┘
```

读取流程：
1. `MessageLog.findSegmentIndexForOffset(targetOffset)` — 二分找到目标段
2. `OffsetIndex.lookup(targetOffset)` — 二分索引，得到最近物理位置
3. `LogSegment.readFrom(position, maxCount)` — 顺序扫描到目标偏移，读取记录
4. 若需跨段读取，`MessageLog.read` 会自动遍历后续段补足 `maxCount`

#### 保留策略（Retention）

| 维度 | 配置项 | 说明 |
|------|--------|------|
| 时间 | `retentionMs` | 删除最后修改时间早于阈值的段 |
| 大小 | `retentionBytes` | 从最旧段开始删除，直到总大小低于阈值 |

活跃段（最新段）永不删除。

---

### 2.4 协议层

#### 帧格式（Wire Frame）

```
 0       1       2       3       4       5       6      7+
┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬────────────┐
│ MAGIC (2B)    │ TYPE  │        LENGTH (4B, BE)        │ PAYLOAD... │
│  0x5A 0x4D   │ (1B)  │                               │ (JSON)     │
└───────┴───────┴───────┴───────┴───────┴───────┴───────┴────────────┘
```

- **MAGIC**：`0x5A4D`（ASCII "ZM"），用于识别协议帧
- **TYPE**：命令类型字节（见下表）
- **LENGTH**：负载字节数，大端序 32 位有符号整数，最大 64 MB
- **PAYLOAD**：UTF-8 JSON 对象

#### 命令类型字节

| 字节 | 名称 | 方向 | 说明 |
|------|------|------|------|
| `0x01` | PUBLISH | C→B | 发布消息 |
| `0x02` | SUBSCRIBE | C→B | 订阅主题分区 |
| `0x03` | UNSUBSCRIBE | C→B | 取消订阅 |
| `0x04` | FETCH | C→B | 拉取消息（支持长轮询）|
| `0x05` | ACK | C→B | 提交偏移量 |
| `0x06` | CREATE_TOPIC | C→B | 创建主题 |
| `0x07` | DELETE_TOPIC | C→B | 删除主题 |
| `0x08` | LIST_TOPICS | C→B | 列举所有主题 |
| `0x09` | TOPIC_INFO | C→B | 查询主题详情 |
| `0x0A` | PING | C→B | 心跳请求 |
| `0x0B` | PONG | B→C | 心跳响应 |
| `0x0C` | RESPONSE_OK | B→C | 成功响应 |
| `0x0D` | RESPONSE_ERROR | B→C | 错误响应 |
| `0x0E` | MESSAGE_BATCH | B→C | 批量消息推送 |

---

## 3. 关键流程

### 3.1 发布流程

```
Producer.send(topic, payload)
  └─ MQClient.sendAndReceive(Publish)
       └─ [TCP] → Broker ConnectionHandler
            └─ handlePublish()
                 ├─ 自动创建主题（如不存在）
                 ├─ 路由分区（Murmur2(key) % numPartitions）
                 ├─ MAPPER.writeValueAsBytes(message)  ← JSON 序列化
                 ├─ MessageLog.append(bytes)           ← 写入 .log 文件
                 ├─ notifyFetchWaiters()               ← 唤醒长轮询等待者
                 └─ 返回 ResponseOk { offset }
```

### 3.2 消费流程

```
Consumer.poll(maxWaitMs)
  └─ MQClient.sendAndReceive(Fetch{fromOffset, maxWaitMs})
       └─ [TCP] → Broker ConnectionHandler
            └─ handleFetch()
                 ├─ fromOffset < 0 → getCommittedOffset() + 1
                 ├─ MessageLog.read(fromOffset, maxCount)
                 ├─ 若空 → awaitNewMessages(maxWaitMs) 长轮询
                 ├─ 重建消息逻辑偏移（fromOffset + i）
                 └─ 返回 MessageBatch

Consumer.commitSync()
  └─ MQClient.sendAndReceive(Ack{topic, partition, groupId, lastOffset})
       └─ Broker handleAck()
            └─ SubscriptionManager.commitOffset()
                 ├─ 内存更新（ConcurrentHashMap）
                 └─ 持久化 offsets.json
```

### 3.3 长轮询机制

```
                  Consumer A（无新消息时阻塞）
                       │
handleFetch()          │
  read() → 空           │
  computeIfAbsent("topic:partition", CountDownLatch(1))
  latch.await(maxWaitMs)        ◄──── 挂起（cheap on virtual thread）
                                │
  Producer.send() → append() → notifyFetchWaiters()
                                │
  fetchLatches.remove(key)      │
  latch.countDown()   ──────────►  唤醒
                                │
  re-read() → 返回新消息         │
```

---

## 4. Java 21 特性应用

| 特性 | 应用位置 | 说明 |
|------|---------|------|
| 虚拟线程 | `Broker.acceptLoop`、`MQClient.readLoop` | 每连接一线程，无阻塞成本 |
| Records | `Message`、`Topic`、`BrokerConfig`、所有 Command 子类型 | 不可变值对象，自动生成 accessor/equals/hashCode |
| Sealed Interfaces | `Command` | 14 个子类型，编译器保证 switch 完备性 |
| Pattern Matching Switch | `ConnectionHandler.dispatch`、`CommandEncoder.typeOf` | 无需强转，类型安全 |
| Text Blocks | `Main.java` 启动 Banner | 多行字符串字面量 |
| `SequencedCollection` API | `segments.getLast()`、`segments.removeFirst()` | Java 21 新增序列集合方法 |

---

## 5. 线程模型

```
JVM 进程
├── main 线程              → 阻塞等待（Thread.join）
├── zmq-acceptor           → 虚拟线程，循环 accept
├── zmq-conn-<uuid> × N    → 虚拟线程，每个客户端连接一个
├── zmq-client-reader × M  → 虚拟线程，每个 MQClient 一个（测试进程内）
└── zmq-scheduler          → 平台线程，定时保留清理 + 心跳检测
```

虚拟线程在阻塞 I/O（Socket 读写）时自动卸载载体线程，无需线程池调优。

---

## 6. 持久化文件布局

```
<dataDir>/
├── topics.json                    # 所有主题的元数据（name/partitions/retention）
├── offsets.json                   # 消费组已提交偏移量
└── <topicName>/
    └── <partitionIndex>/
        ├── 00000000000000000000.log    # 日志段（追加写，CRC32 校验）
        ├── 00000000000000000000.index  # 稀疏偏移索引（16 字节/条目）
        ├── 00000000000000040960.log    # 段滚动后的新段（基偏移量 = 滚动时 nextOffset）
        └── 00000000000000040960.index
```

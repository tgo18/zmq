package com.zmq.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zmq.message.Message;
import com.zmq.message.Topic;
import com.zmq.storage.MessageLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Manages topic lifecycle, message logs, and partition routing.
 *
 * Thread-safe: uses ConcurrentHashMap for topic registry; per-partition
 * locking is handled inside {@link MessageLog}.
 */
public class TopicManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String METADATA_FILE = "topics.json";

    private final Path dataDir;
    private final long maxSegmentBytes;

    /** topic name → TopicState */
    private final ConcurrentHashMap<String, TopicState> topics = new ConcurrentHashMap<>();

    /** (topic, partition) → CountDownLatch for long-poll wake-up */
    private final ConcurrentHashMap<String, CountDownLatch> fetchLatches = new ConcurrentHashMap<>();

    record TopicState(Topic metadata, Map<Integer, MessageLog> partitions) {}

    public TopicManager(Path dataDir, long maxSegmentBytes) {
        this.dataDir = dataDir;
        this.maxSegmentBytes = maxSegmentBytes;
    }

    public Topic createTopic(String name, int numPartitions,
                              long retentionBytes, long retentionMs) throws IOException {
        if (topics.containsKey(name)) {
            return topics.get(name).metadata(); // idempotent
        }

        Duration maxAge = retentionMs > 0 ? Duration.ofMillis(retentionMs) : null;
        Topic.RetentionPolicy policy = new Topic.RetentionPolicy(
                retentionBytes, maxAge, (int) Math.min(maxSegmentBytes, Integer.MAX_VALUE));
        Topic topic = Topic.of(name, numPartitions, policy);

        Map<Integer, MessageLog> partitions = new HashMap<>();
        for (int p = 0; p < numPartitions; p++) {
            Path partDir = dataDir.resolve(name).resolve(String.valueOf(p));
            partitions.put(p, new MessageLog(partDir, maxSegmentBytes));
        }

        topics.put(name, new TopicState(topic, partitions));
        persistMetadata();
        log.info("Created topic '{}' with {} partition(s)", name, numPartitions);
        return topic;
    }

    public void deleteTopic(String name) throws IOException {
        TopicState state = topics.remove(name);
        if (state == null) throw new IllegalArgumentException("Topic not found: " + name);

        for (MessageLog ml : state.partitions().values()) {
            ml.close();
        }
        // Delete directory tree
        Path topicDir = dataDir.resolve(name);
        if (Files.exists(topicDir)) {
            try (var walk = Files.walk(topicDir)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
            }
        }
        persistMetadata();
        log.info("Deleted topic '{}'", name);
    }

    public MessageLog getLog(String topic, int partition) {
        TopicState state = topics.get(topic);
        if (state == null) throw new IllegalArgumentException("Topic not found: " + topic);
        MessageLog ml = state.partitions().get(partition);
        if (ml == null) throw new IllegalArgumentException("Partition " + partition + " not found in topic " + topic);
        return ml;
    }

    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    public Topic getTopic(String name) {
        TopicState state = topics.get(name);
        return state != null ? state.metadata() : null;
    }

    public List<Topic> listTopics() {
        return topics.values().stream().map(TopicState::metadata).toList();
    }

    /**
     * Route a message key to a partition number using Murmur2-inspired hash.
     * Returns 0 if the topic has only one partition or key is null.
     */
    public int routeToPartition(String topic, String key) {
        TopicState state = topics.get(topic);
        if (state == null) return 0;
        int numPartitions = state.metadata().numPartitions();
        if (numPartitions == 1 || key == null || key.isEmpty()) return 0;
        return Math.abs(murmur2(key.getBytes(java.nio.charset.StandardCharsets.UTF_8))) % numPartitions;
    }

    /**
     * Append a pre-built Message to the appropriate partition log.
     * Notifies any waiting FETCH callers for this partition.
     */
    public long append(Message message, byte[] serializedBytes) throws IOException {
        MessageLog ml = getLog(message.topic(), message.partition());
        long offset = ml.append(serializedBytes);
        notifyFetchWaiters(message.topic(), message.partition());
        return offset;
    }

    /**
     * Block up to {@code maxWaitMs} for new messages on the given (topic, partition).
     * Used by long-poll FETCH.
     */
    public boolean awaitNewMessages(String topic, int partition, long maxWaitMs)
            throws InterruptedException {
        String key = topic + ":" + partition;
        CountDownLatch latch = fetchLatches.computeIfAbsent(key, k -> new CountDownLatch(1));
        return latch.await(maxWaitMs, TimeUnit.MILLISECONDS);
    }

    public void flushAll() throws IOException {
        for (TopicState state : topics.values()) {
            for (MessageLog ml : state.partitions().values()) {
                ml.flush();
            }
        }
    }

    public void applyRetentionAll() {
        for (TopicState state : topics.values()) {
            Topic.RetentionPolicy policy = state.metadata().retentionPolicy();
            for (Map.Entry<Integer, MessageLog> entry : state.partitions().entrySet()) {
                try {
                    entry.getValue().applyRetention(policy.maxAge(), policy.maxBytes());
                } catch (IOException e) {
                    log.warn("Retention cleanup failed for {}/{}", state.metadata().name(), entry.getKey(), e);
                }
            }
        }
    }

    /** Persist topic metadata to disk so topics survive broker restarts. */
    public void persistMetadata() throws IOException {
        Path metaFile = dataDir.resolve(METADATA_FILE);
        List<Map<String, Object>> topicList = new ArrayList<>();
        for (TopicState state : topics.values()) {
            Topic t = state.metadata();
            Map<String, Object> m = new HashMap<>();
            m.put("name", t.name());
            m.put("numPartitions", t.numPartitions());
            m.put("retentionBytes", t.retentionPolicy().maxBytes());
            m.put("retentionMs", t.retentionPolicy().maxAge() != null
                    ? t.retentionPolicy().maxAge().toMillis() : -1L);
            topicList.add(m);
        }
        MAPPER.writeValue(metaFile.toFile(), topicList);
    }

    /** Restore topic metadata and reopen all MessageLogs on broker startup. */
    @SuppressWarnings("unchecked")
    public void loadMetadata() throws IOException {
        Path metaFile = dataDir.resolve(METADATA_FILE);
        if (!Files.exists(metaFile)) return;

        List<Map<String, Object>> topicList = MAPPER.readValue(metaFile.toFile(), List.class);
        for (Map<String, Object> m : topicList) {
            String name = (String) m.get("name");
            int numPartitions = ((Number) m.get("numPartitions")).intValue();
            long retentionBytes = ((Number) m.getOrDefault("retentionBytes", -1L)).longValue();
            long retentionMs = ((Number) m.getOrDefault("retentionMs", 604800000L)).longValue();
            try {
                createTopic(name, numPartitions, retentionBytes, retentionMs);
            } catch (Exception e) {
                log.warn("Failed to restore topic '{}'", name, e);
            }
        }
        log.info("Loaded {} topic(s) from metadata", topics.size());
    }

    @Override
    public void close() throws IOException {
        for (TopicState state : topics.values()) {
            for (MessageLog ml : state.partitions().values()) {
                try { ml.close(); } catch (IOException e) {
                    log.warn("Error closing log for topic {}", state.metadata().name(), e);
                }
            }
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void notifyFetchWaiters(String topic, int partition) {
        String key = topic + ":" + partition;
        CountDownLatch latch = fetchLatches.remove(key);
        if (latch != null) latch.countDown();
    }

    /** Murmur2 hash (same algorithm as Kafka's default partitioner). */
    private static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747B28C;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff)
                    + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16)
                    + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int remaining = length % 4;
        int base = length4 * 4;
        switch (remaining) {
            case 3: h ^= (data[base + 2] & 0xff) << 16; // fall through
            case 2: h ^= (data[base + 1] & 0xff) << 8;  // fall through
            case 1: h ^= (data[base] & 0xff); h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    public Collection<TopicState> topicStates() {
        return topics.values();
    }
}

package com.zmq.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks consumer group offsets and active group memberships.
 *
 * Thread-safe via ConcurrentHashMap; offset updates are atomic per partition.
 */
public class SubscriptionManager {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String OFFSETS_FILE = "offsets.json";

    private final Path dataDir;

    /**
     * groupId → topic → partition → committed offset
     * All maps are ConcurrentHashMap for thread-safe reads.
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>>
            committedOffsets = new ConcurrentHashMap<>();

    /** groupId → set of connection IDs (for membership tracking) */
    private final ConcurrentHashMap<String, Set<String>> groupMembers = new ConcurrentHashMap<>();

    public SubscriptionManager(Path dataDir) {
        this.dataDir = dataDir;
    }

    /**
     * Returns the committed offset for the given group/topic/partition.
     * Returns -1 if no offset has been committed yet (consumer should start from 0).
     */
    public long getCommittedOffset(String groupId, String topic, int partition) {
        var byTopic = committedOffsets.get(groupId);
        if (byTopic == null) return -1L;
        var byPartition = byTopic.get(topic);
        if (byPartition == null) return -1L;
        return byPartition.getOrDefault(partition, -1L);
    }

    /**
     * Commit the given offset for a consumer group.
     * The offset is the last successfully processed message offset.
     * Next fetch will start from offset + 1.
     */
    public void commitOffset(String groupId, String topic, int partition, long offset) {
        committedOffsets
                .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .put(partition, offset);

        try {
            persistOffsets();
        } catch (IOException e) {
            log.warn("Failed to persist offsets after commit", e);
        }
    }

    public void addMember(String groupId, String connectionId) {
        groupMembers.computeIfAbsent(groupId, k -> ConcurrentHashMap.newKeySet()).add(connectionId);
    }

    public void removeMember(String groupId, String connectionId) {
        Set<String> members = groupMembers.get(groupId);
        if (members != null) {
            members.remove(connectionId);
            if (members.isEmpty()) groupMembers.remove(groupId);
        }
    }

    public Set<String> groupMembers(String groupId) {
        return groupMembers.getOrDefault(groupId, Set.of());
    }

    public Set<String> groupIds() {
        return committedOffsets.keySet();
    }

    /**
     * Persist committed offsets to disk for crash recovery.
     */
    public synchronized void persistOffsets() throws IOException {
        Path file = dataDir.resolve(OFFSETS_FILE);
        MAPPER.writeValue(file.toFile(), committedOffsets);
    }

    /**
     * Restore committed offsets on broker startup.
     */
    @SuppressWarnings("unchecked")
    public void loadOffsets() throws IOException {
        Path file = dataDir.resolve(OFFSETS_FILE);
        if (!Files.exists(file)) return;

        Map<String, Map<String, Map<String, Number>>> raw =
                MAPPER.readValue(file.toFile(), Map.class);

        for (var groupEntry : raw.entrySet()) {
            String groupId = groupEntry.getKey();
            for (var topicEntry : groupEntry.getValue().entrySet()) {
                String topic = topicEntry.getKey();
                for (var partEntry : topicEntry.getValue().entrySet()) {
                    int partition = Integer.parseInt(partEntry.getKey());
                    long offset = partEntry.getValue().longValue();
                    committedOffsets
                            .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                            .put(partition, offset);
                }
            }
        }
        log.info("Loaded offsets for {} consumer group(s)", committedOffsets.size());
    }

    /** Build a summary of group lag for admin reporting. */
    public Map<String, Object> groupInfo(String groupId, TopicManager topicManager) {
        Map<String, Object> result = new HashMap<>();
        var byTopic = committedOffsets.get(groupId);
        if (byTopic == null) return result;

        for (var topicEntry : byTopic.entrySet()) {
            String topic = topicEntry.getKey();
            Map<Integer, Long> lags = new HashMap<>();
            for (var partEntry : topicEntry.getValue().entrySet()) {
                int partition = partEntry.getKey();
                long committed = partEntry.getValue();
                try {
                    long endOffset = topicManager.getLog(topic, partition).nextOffset();
                    lags.put(partition, endOffset - committed - 1);
                } catch (Exception ignored) {
                    lags.put(partition, -1L);
                }
            }
            result.put(topic, lags);
        }
        return result;
    }

    public List<String> listGroups() {
        return List.copyOf(committedOffsets.keySet());
    }
}

package com.zmq;

import com.zmq.broker.TopicManager;
import com.zmq.message.Message;
import com.zmq.message.Topic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TopicManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void testCreateAndGetTopic() throws Exception {
        TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
        Topic topic = tm.createTopic("orders", 3, -1L, 86400000L);

        assertEquals("orders", topic.name());
        assertEquals(3, topic.numPartitions());
        assertTrue(tm.topicExists("orders"));
        assertNotNull(tm.getLog("orders", 0));
        assertNotNull(tm.getLog("orders", 1));
        assertNotNull(tm.getLog("orders", 2));

        tm.close();
    }

    @Test
    void testCreateTopicIdempotent() throws Exception {
        TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
        Topic t1 = tm.createTopic("events", 2, -1L, 86400000L);
        Topic t2 = tm.createTopic("events", 2, -1L, 86400000L);
        assertEquals(t1.name(), t2.name());
        assertEquals(1, tm.listTopics().size());
        tm.close();
    }

    @Test
    void testPartitionRouting() throws Exception {
        TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
        tm.createTopic("routed", 4, -1L, 86400000L);

        // Same key always maps to same partition
        int p1 = tm.routeToPartition("routed", "order-123");
        int p2 = tm.routeToPartition("routed", "order-123");
        assertEquals(p1, p2);

        // Null key → partition 0
        assertEquals(0, tm.routeToPartition("routed", null));

        // Different keys can map to different partitions (not guaranteed but statistically likely)
        // Just verify the partition is in valid range
        for (String key : List.of("a", "b", "c", "d", "e")) {
            int p = tm.routeToPartition("routed", key);
            assertTrue(p >= 0 && p < 4, "Partition " + p + " out of range for key " + key);
        }

        tm.close();
    }

    @Test
    void testAppendAndRead() throws Exception {
        TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
        tm.createTopic("data", 1, -1L, 86400000L);

        Message msg = new Message(UUID.randomUUID().toString(), "data", 0, -1L,
                "hello".getBytes(), Map.of(), java.time.Instant.now(), null);
        byte[] bytes = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(msg);

        long offset = tm.append(msg, bytes);
        assertEquals(0L, offset);

        List<byte[]> records = tm.getLog("data", 0).read(0, 10);
        assertEquals(1, records.size());

        tm.close();
    }

    @Test
    void testDeleteTopic() throws Exception {
        TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
        tm.createTopic("temp", 1, -1L, 86400000L);
        assertTrue(tm.topicExists("temp"));

        tm.deleteTopic("temp");
        assertFalse(tm.topicExists("temp"));
        tm.close();
    }

    @Test
    void testMetadataPersistence() throws Exception {
        {
            TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
            tm.createTopic("persistent-topic", 2, -1L, 86400000L);
            tm.persistMetadata();
            tm.close();
        }

        // Reopen and verify topic is restored
        {
            TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
            tm.loadMetadata();
            assertTrue(tm.topicExists("persistent-topic"));
            assertEquals(2, tm.getTopic("persistent-topic").numPartitions());
            tm.close();
        }
    }

    @Test
    void testListTopics() throws Exception {
        TopicManager tm = new TopicManager(tempDir, 10 * 1024 * 1024L);
        tm.createTopic("alpha", 1, -1L, 86400000L);
        tm.createTopic("beta", 2, -1L, 86400000L);
        tm.createTopic("gamma", 3, -1L, 86400000L);

        List<String> names = tm.listTopics().stream().map(Topic::name).sorted().toList();
        assertEquals(List.of("alpha", "beta", "gamma"), names);

        tm.close();
    }
}

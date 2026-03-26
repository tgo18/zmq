package com.zmq;

import com.zmq.broker.Broker;
import com.zmq.broker.BrokerConfig;
import com.zmq.client.Consumer;
import com.zmq.client.MQClient;
import com.zmq.client.Producer;
import com.zmq.message.Message;
import com.zmq.protocol.Command;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests against a real broker started on an ephemeral port.
 */
class BrokerIntegrationTest {

    @TempDir
    Path tempDir;

    private Broker broker;
    private int port;

    @BeforeEach
    void startBroker() throws Exception {
        BrokerConfig config = new BrokerConfig(
                0,      // port=0 → OS assigns a free port
                0,
                tempDir,
                1,
                256 * 1024 * 1024L,
                7 * 24 * 60 * 60 * 1000L,
                -1L,
                30_000,
                90_001,
                1000
        );
        broker = new Broker(config);
        broker.start();
        port = broker.port();
    }

    @AfterEach
    void stopBroker() throws Exception {
        broker.close();
    }

    private MQClient connect() throws Exception {
        MQClient client = new MQClient("localhost", port);
        client.connect();
        return client;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void testPublishAndConsume() throws Exception {
        try (MQClient pc = connect(); MQClient cc = connect()) {
            Producer producer = pc.createProducer();
            Consumer consumer = cc.createConsumer("test-group");

            // Create topic explicitly
            Command response = pc.sendAndReceive(new Command.CreateTopic("orders", 1, -1L, 86400000L));
            assertTrue(response instanceof Command.ResponseOk, "CreateTopic failed: " + response);

            consumer.subscribe("orders");

            // Publish 3 messages
            for (int i = 0; i < 3; i++) {
                long offset = producer.send("orders", ("msg-" + i).getBytes());
                assertTrue(offset >= 0);
            }

            // Poll and collect messages
            List<Message> received = new ArrayList<>();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                received.addAll(consumer.poll(500));
                return received.size() >= 3;
            });

            assertEquals(3, received.size());
            assertEquals("msg-0", new String(received.get(0).payload()));
            assertEquals("msg-2", new String(received.get(2).payload()));
        }
    }

    @Test
    void testAtLeastOnceDelivery() throws Exception {
        try (MQClient pc = connect()) {
            Producer producer = pc.createProducer();
            // Create topic
            pc.sendAndReceive(new Command.CreateTopic("payments", 1, -1L, 86400000L));

            // Publish 5 messages
            for (int i = 0; i < 5; i++) {
                producer.send("payments", ("pay-" + i).getBytes());
            }
        }

        // First consumer reads but does NOT ack → crashes (close without commitSync)
        try (MQClient cc = connect()) {
            Consumer consumer = cc.createConsumer("billing");
            consumer.subscribe("payments");

            List<Message> batch1 = new ArrayList<>();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                batch1.addAll(consumer.poll(500));
                return batch1.size() >= 5;
            });
            assertEquals(5, batch1.size());
            // No commitSync — intentional "crash"
        }

        // Second consumer with same groupId should see all 5 messages again
        try (MQClient cc2 = connect()) {
            Consumer consumer2 = cc2.createConsumer("billing");
            consumer2.subscribe("payments");

            List<Message> batch2 = new ArrayList<>();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                batch2.addAll(consumer2.poll(500));
                return batch2.size() >= 5;
            });
            assertEquals(5, batch2.size());
            assertEquals("pay-0", new String(batch2.get(0).payload()));

            // Now ack
            consumer2.commitSync();
        }

        // Third consumer should see nothing (offsets committed)
        try (MQClient cc3 = connect()) {
            Consumer consumer3 = cc3.createConsumer("billing");
            consumer3.subscribe("payments");
            List<Message> batch3 = consumer3.poll(500);
            assertTrue(batch3.isEmpty(), "Expected empty after commit, got: " + batch3.size());
        }
    }

    @Test
    void testConsumerGroupOffset() throws Exception {
        try (MQClient pc = connect()) {
            pc.sendAndReceive(new Command.CreateTopic("events", 1, -1L, 86400000L));
            Producer producer = pc.createProducer();

            for (int i = 0; i < 10; i++) {
                producer.send("events", ("ev-" + i).getBytes());
            }
        }

        // Consumer A reads first 5 and commits
        try (MQClient ca = connect()) {
            Consumer consumerA = ca.createConsumer("group-a");
            consumerA.subscribe("events");

            List<Message> msgs = new ArrayList<>();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                msgs.addAll(consumerA.poll(500));
                return msgs.size() >= 5;
            });

            // Seek back to simulate reading only first 5
            consumerA.seek("events", 0, 5L);
            consumerA.commitSync(); // commits offset 4
        }

        // Consumer B in different group sees all 10 from the start
        try (MQClient cb = connect()) {
            Consumer consumerB = cb.createConsumer("group-b");
            consumerB.subscribe("events");

            List<Message> msgsB = new ArrayList<>();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                msgsB.addAll(consumerB.poll(500));
                return msgsB.size() >= 10;
            });
            assertEquals(10, msgsB.size());
        }
    }

    @Test
    void testTopicCreationAndListing() throws Exception {
        try (MQClient client = connect()) {
            // Create two topics
            Command r1 = client.sendAndReceive(new Command.CreateTopic("topic-a", 2, -1L, 86400000L));
            Command r2 = client.sendAndReceive(new Command.CreateTopic("topic-b", 1, -1L, 86400000L));
            assertTrue(r1 instanceof Command.ResponseOk);
            assertTrue(r2 instanceof Command.ResponseOk);

            // List topics
            Command listResp = client.sendAndReceive(new Command.ListTopics());
            assertTrue(listResp instanceof Command.ResponseOk ok
                    && ok.data().containsKey("topics"));
        }
    }

    @Test
    void testTopicInfo() throws Exception {
        try (MQClient client = connect()) {
            client.sendAndReceive(new Command.CreateTopic("info-topic", 3, -1L, 86400000L));
            Command info = client.sendAndReceive(new Command.TopicInfo("info-topic"));
            assertTrue(info instanceof Command.ResponseOk ok
                    && ok.data().get("numPartitions") != null);
            assertEquals(3, ((Number) ((Command.ResponseOk) info).data().get("numPartitions")).intValue());
        }
    }

    @Test
    void testPingPong() throws Exception {
        try (MQClient client = connect()) {
            long ts = System.currentTimeMillis();
            Command response = client.sendAndReceive(new Command.Ping(ts));
            assertTrue(response instanceof Command.Pong pong && pong.timestamp() == ts,
                    "Expected Pong with same timestamp, got: " + response);
        }
    }

    @Test
    void testDeleteTopic() throws Exception {
        try (MQClient client = connect()) {
            client.sendAndReceive(new Command.CreateTopic("temp-topic", 1, -1L, 86400000L));

            Command deleteResp = client.sendAndReceive(new Command.DeleteTopic("temp-topic"));
            assertTrue(deleteResp instanceof Command.ResponseOk);

            // Publishing to deleted topic auto-creates it, so test listing instead
            Command listResp = client.sendAndReceive(new Command.ListTopics());
            Command.ResponseOk ok = (Command.ResponseOk) listResp;
            @SuppressWarnings("unchecked")
            List<String> topics = (List<String>) ok.data().get("topics");
            assertFalse(topics.contains("temp-topic"));
        }
    }

    @Test
    void testConcurrentProducers() throws Exception {
        try (MQClient setup = connect()) {
            setup.sendAndReceive(new Command.CreateTopic("concurrent", 1, -1L, 86400000L));
        }

        int numProducers = 10;
        int msgsPerProducer = 20;
        AtomicLong totalSent = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(numProducers);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < numProducers; i++) {
                final int producerId = i;
                executor.submit(() -> {
                    try (MQClient client = connect()) {
                        Producer producer = client.createProducer();
                        for (int j = 0; j < msgsPerProducer; j++) {
                            producer.send("concurrent", ("p" + producerId + "-m" + j).getBytes());
                            totalSent.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                    return null;
                });
            }
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(numProducers * msgsPerProducer, totalSent.get());

        // Verify all messages are stored
        try (MQClient cc = connect()) {
            Consumer consumer = cc.createConsumer("verify-group");
            consumer.subscribe("concurrent");
            consumer.seek("concurrent", 0, 0L);

            List<Message> all = new ArrayList<>();
            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                all.addAll(consumer.poll(500));
                return all.size() >= numProducers * msgsPerProducer;
            });
            assertEquals(numProducers * msgsPerProducer, all.size());
        }
    }
}

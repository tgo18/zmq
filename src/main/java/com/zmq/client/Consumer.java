package com.zmq.client;

import com.zmq.message.Message;
import com.zmq.protocol.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pull-based consumer API built on {@link MQClient}.
 *
 * At-least-once semantics: offsets are only advanced after explicit {@link #ack}.
 * If the consumer crashes before ack, re-connecting with the same groupId will
 * resume from the last committed offset.
 */
public class Consumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final MQClient client;
    private final String groupId;

    /** topic → partition → next fetch offset (local cursor, ahead of committed) */
    private final Map<String, Map<Integer, Long>> fetchOffsets = new HashMap<>();

    public Consumer(MQClient client, String groupId) {
        this.client = client;
        this.groupId = groupId;
    }

    /**
     * Subscribe to partition 0 of the given topic.
     * The broker will resume from the last committed offset for this groupId.
     */
    public void subscribe(String topic) throws IOException {
        subscribe(topic, 0);
    }

    /**
     * Subscribe to a specific partition. The broker will acknowledge and return
     * the last committed offset (or 0 if none). The local cursor is set accordingly.
     */
    public void subscribe(String topic, int partition) throws IOException {
        Command.Subscribe sub = new Command.Subscribe(topic, partition, groupId);
        Command response = client.sendAndReceive(sub);
        if (response instanceof Command.ResponseError err) {
            throw new IOException("Subscribe failed [" + err.errorCode() + "]: " + err.message());
        }
        // Local cursor: will be set from committed offset on first poll
        fetchOffsets.computeIfAbsent(topic, k -> new HashMap<>()).put(partition, -1L);
        log.debug("Subscribed to {}:{} as group '{}'", topic, partition, groupId);
    }

    /**
     * Poll for messages across all subscribed (topic, partition) pairs.
     *
     * @param maxWaitMs how long to wait for messages if none are immediately available
     * @return list of received messages (may be empty)
     */
    public List<Message> poll(long maxWaitMs) throws IOException {
        List<Message> results = new ArrayList<>();

        for (Map.Entry<String, Map<Integer, Long>> topicEntry : fetchOffsets.entrySet()) {
            String topic = topicEntry.getKey();
            for (Map.Entry<Integer, Long> partEntry : topicEntry.getValue().entrySet()) {
                int partition = partEntry.getKey();
                long cursor = partEntry.getValue();
                // -1 means "use server-side committed offset"
                long fromOffset = cursor >= 0 ? cursor : -1L;

                Command.Fetch fetchCmd = new Command.Fetch(
                        topic, partition, groupId, fromOffset, 100, maxWaitMs);
                Command response = client.sendAndReceive(fetchCmd);

                switch (response) {
                    case Command.MessageBatch batch -> {
                        List<Message> msgs = batch.messages();
                        results.addAll(msgs);
                        if (!msgs.isEmpty()) {
                            // Advance local cursor past the last received message
                            long lastOffset = msgs.getLast().offset();
                            partEntry.setValue(lastOffset + 1);
                        }
                    }
                    case Command.ResponseError err ->
                            throw new IOException("Fetch failed [" + err.errorCode() + "]: " + err.message());
                    default -> { /* empty batch */ }
                }
            }
        }
        return results;
    }

    /**
     * Commit the current fetch cursors to the broker for all subscribed partitions.
     * Call this after successfully processing a batch of messages.
     */
    public void commitSync() throws IOException {
        for (Map.Entry<String, Map<Integer, Long>> topicEntry : fetchOffsets.entrySet()) {
            String topic = topicEntry.getKey();
            for (Map.Entry<Integer, Long> partEntry : topicEntry.getValue().entrySet()) {
                long nextToFetch = partEntry.getValue();
                if (nextToFetch <= 0) continue; // nothing consumed yet
                long lastConsumed = nextToFetch - 1;

                Command.Ack ack = new Command.Ack(topic, partEntry.getKey(), groupId, lastConsumed);
                Command response = client.sendAndReceive(ack);
                if (response instanceof Command.ResponseError err) {
                    throw new IOException("Ack failed [" + err.errorCode() + "]: " + err.message());
                }
            }
        }
    }

    /**
     * Explicitly ack a single message offset.
     */
    public void ack(String topic, int partition, long offset) throws IOException {
        Command.Ack ack = new Command.Ack(topic, partition, groupId, offset);
        Command response = client.sendAndReceive(ack);
        if (response instanceof Command.ResponseError err) {
            throw new IOException("Ack failed [" + err.errorCode() + "]: " + err.message());
        }
        // Advance local cursor
        fetchOffsets.computeIfAbsent(topic, k -> new HashMap<>())
                .merge(partition, offset + 1, Math::max);
    }

    /**
     * Seek to a specific offset on the given (topic, partition).
     * Next poll will fetch from this offset.
     */
    public void seek(String topic, int partition, long offset) {
        fetchOffsets.computeIfAbsent(topic, k -> new HashMap<>()).put(partition, offset);
    }

    @Override
    public void close() {
        // Consumer does not own the MQClient lifecycle.
    }
}

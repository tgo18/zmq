package com.zmq.client;

import com.zmq.protocol.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * High-level publish API built on {@link MQClient}.
 *
 * Supports single sends and simple batching (collect locally, flush together).
 */
public class Producer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final MQClient client;

    public Producer(MQClient client) {
        this.client = client;
    }

    /**
     * Send a message to a topic. Partition is auto-selected (key-based routing on broker).
     *
     * @return the assigned broker offset
     */
    public long send(String topic, byte[] payload) throws IOException {
        return send(topic, null, payload, Map.of());
    }

    /**
     * Send a message with a routing key and optional headers.
     *
     * @return the assigned broker offset
     */
    public long send(String topic, String key, byte[] payload,
                     Map<String, String> headers) throws IOException {
        Command.Publish cmd = new Command.Publish(topic, -1, key, payload, headers, 5000L);
        Command response = client.sendAndReceive(cmd);
        return switch (response) {
            case Command.ResponseOk ok -> ((Number) ok.data().get("offset")).longValue();
            case Command.ResponseError err ->
                    throw new IOException("Publish failed [" + err.errorCode() + "]: " + err.message());
            default -> throw new IOException("Unexpected response: " + response.getClass().getSimpleName());
        };
    }

    /**
     * Send a batch of messages to the same topic.
     *
     * @return list of assigned offsets in the same order as {@code messages}
     */
    public List<Long> sendBatch(String topic, List<byte[]> messages) throws IOException {
        List<Long> offsets = new ArrayList<>(messages.size());
        for (byte[] payload : messages) {
            offsets.add(send(topic, payload));
        }
        return offsets;
    }

    @Override
    public void close() {
        // Producer does not own the MQClient lifecycle; nothing to close here.
    }
}

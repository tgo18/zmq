package com.zmq.message;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable message record — the core data unit flowing through the broker.
 * offset is -1 before broker assignment; set during log append.
 */
public record Message(
        String id,
        String topic,
        int partition,
        long offset,
        byte[] payload,
        Map<String, String> headers,
        @JsonSerialize(using = InstantMillisSerializer.class)
        @JsonDeserialize(using = InstantMillisDeserializer.class)
        Instant timestamp,
        String key
) {
    public Message {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("id must not be blank");
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic must not be blank");
        if (partition < 0) throw new IllegalArgumentException("partition must be >= 0");
        if (payload == null) payload = new byte[0];
        if (headers == null) headers = Collections.emptyMap();
        if (timestamp == null) timestamp = Instant.now();
    }

    public static Message create(String id, String topic, int partition,
                                  byte[] payload, Map<String, String> headers, String key) {
        return new Message(id, topic, partition, -1L, payload, headers, Instant.now(), key);
    }

    /** Return a copy with the assigned broker offset. */
    public Message withOffset(long offset) {
        return new Message(id, topic, partition, offset, payload, headers, timestamp, key);
    }
}

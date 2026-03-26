package com.zmq.message;

import java.time.Duration;
import java.time.Instant;

/**
 * Topic metadata record.
 */
public record Topic(
        String name,
        int numPartitions,
        RetentionPolicy retentionPolicy,
        Instant createdAt
) {
    public Topic {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("topic name must not be blank");
        if (numPartitions < 1) throw new IllegalArgumentException("numPartitions must be >= 1");
        if (retentionPolicy == null) retentionPolicy = RetentionPolicy.DEFAULT;
        if (createdAt == null) createdAt = Instant.now();
    }

    public static Topic of(String name, int partitions) {
        return new Topic(name, partitions, RetentionPolicy.DEFAULT, Instant.now());
    }

    public static Topic of(String name, int partitions, RetentionPolicy policy) {
        return new Topic(name, partitions, policy, Instant.now());
    }

    /**
     * Retention policy controls how long or how much data to keep per partition.
     * -1 means unlimited.
     */
    public record RetentionPolicy(
            long maxBytes,
            Duration maxAge,
            int maxSegmentBytes
    ) {
        public static final RetentionPolicy DEFAULT = new RetentionPolicy(
                -1L,
                Duration.ofDays(7),
                256 * 1024 * 1024  // 256 MB per segment
        );

        public RetentionPolicy {
            if (maxSegmentBytes < 1024) throw new IllegalArgumentException("maxSegmentBytes must be >= 1024");
        }
    }
}

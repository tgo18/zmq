package com.zmq.broker;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Immutable broker configuration record.
 */
public record BrokerConfig(
        int port,
        int adminPort,
        Path dataDir,
        int defaultPartitions,
        long maxSegmentBytes,
        long retentionMs,
        long retentionBytes,
        int heartbeatIntervalMs,
        int heartbeatTimeoutMs,
        int maxConnections
) {
    public static final BrokerConfig DEFAULT = new BrokerConfig(
            9092,
            9093,
            Paths.get("data"),
            4,
            256L * 1024 * 1024,   // 256 MB per segment
            7L * 24 * 60 * 60 * 1000, // 7 days
            -1L,
            30_000,
            90_000,
            1000
    );

    public BrokerConfig {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("invalid port: " + port);
        if (defaultPartitions < 1) throw new IllegalArgumentException("defaultPartitions must be >= 1");
        if (maxSegmentBytes < 1024) throw new IllegalArgumentException("maxSegmentBytes must be >= 1024");
        if (heartbeatIntervalMs <= 0) throw new IllegalArgumentException("heartbeatIntervalMs must be > 0");
        if (heartbeatTimeoutMs <= heartbeatIntervalMs)
            throw new IllegalArgumentException("heartbeatTimeoutMs must be > heartbeatIntervalMs");
        if (dataDir == null) throw new IllegalArgumentException("dataDir must not be null");
    }
}

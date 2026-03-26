package com.zmq;

import com.zmq.broker.Broker;
import com.zmq.broker.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * ZMQ Message Queue Broker entry point.
 *
 * Usage:
 *   java -jar zmq.jar [--port <port>] [--data <dir>] [--partitions <n>]
 *
 * Defaults: port=9092, data=./data, partitions=4
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        BrokerConfig config = parseArgs(args);

        log.info("""
                ╔══════════════════════════════════════╗
                ║        ZMQ Message Queue             ║
                ║        Java 21 Edition               ║
                ╚══════════════════════════════════════╝
                """);
        log.info("Starting broker on port {} | data dir: {}", config.port(), config.dataDir());

        Broker broker = new Broker(config);
        broker.start();

        // Shutdown hook for graceful termination on SIGTERM/SIGINT
        Runtime.getRuntime().addShutdownHook(
                Thread.ofVirtual().unstarted(() -> {
                    try {
                        log.info("Shutdown signal received");
                        broker.close();
                    } catch (Exception e) {
                        log.error("Error during shutdown", e);
                    }
                })
        );

        // Block main thread until JVM exits
        Thread.currentThread().join();
    }

    private static BrokerConfig parseArgs(String[] args) {
        int port = 9092;
        int adminPort = 9093;
        String dataDir = "data";
        int partitions = 4;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port"       -> port = Integer.parseInt(args[++i]);
                case "--admin-port" -> adminPort = Integer.parseInt(args[++i]);
                case "--data"       -> dataDir = args[++i];
                case "--partitions" -> partitions = Integer.parseInt(args[++i]);
            }
        }

        return new BrokerConfig(
                port, adminPort, Paths.get(dataDir), partitions,
                256L * 1024 * 1024, 7L * 24 * 60 * 60 * 1000, -1L,
                30_000, 90_000, 1000
        );
    }
}

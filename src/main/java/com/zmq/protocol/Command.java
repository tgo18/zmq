package com.zmq.protocol;

import com.zmq.message.Message;

import java.util.List;
import java.util.Map;

/**
 * Sealed interface hierarchy for all protocol commands.
 *
 * Using Java 21 sealed interfaces + records enables exhaustive pattern
 * matching in switch expressions — the compiler enforces that every
 * command type is handled.
 *
 * Wire type bytes are defined in {@link Frame.Type}.
 */
public sealed interface Command permits
        Command.Publish,
        Command.Subscribe,
        Command.Unsubscribe,
        Command.Fetch,
        Command.Ack,
        Command.CreateTopic,
        Command.DeleteTopic,
        Command.ListTopics,
        Command.TopicInfo,
        Command.Ping,
        Command.Pong,
        Command.ResponseOk,
        Command.ResponseError,
        Command.MessageBatch {

    // ── Client → Broker ──────────────────────────────────────────────────────

    record Publish(
            String topic,
            int partition,
            String key,
            byte[] payload,
            Map<String, String> headers,
            long produceTimeoutMs
    ) implements Command {
        public Publish {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic required");
            if (payload == null) payload = new byte[0];
            if (headers == null) headers = Map.of();
            if (produceTimeoutMs <= 0) produceTimeoutMs = 5000L;
        }
    }

    record Subscribe(
            String topic,
            int partition,
            String groupId
    ) implements Command {
        public Subscribe {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic required");
            if (groupId == null || groupId.isBlank()) throw new IllegalArgumentException("groupId required");
        }
    }

    record Unsubscribe(
            String topic,
            String groupId
    ) implements Command {
        public Unsubscribe {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic required");
            if (groupId == null || groupId.isBlank()) throw new IllegalArgumentException("groupId required");
        }
    }

    record Fetch(
            String topic,
            int partition,
            String groupId,
            long fromOffset,
            int maxMessages,
            long maxWaitMs
    ) implements Command {
        public Fetch {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic required");
            if (groupId == null || groupId.isBlank()) throw new IllegalArgumentException("groupId required");
            if (fromOffset < 0) fromOffset = -1L;  // -1 = use committed offset
            if (maxMessages <= 0) maxMessages = 100;
            if (maxWaitMs < 0) maxWaitMs = 0L;
        }
    }

    record Ack(
            String topic,
            int partition,
            String groupId,
            long offset
    ) implements Command {
        public Ack {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic required");
            if (groupId == null || groupId.isBlank()) throw new IllegalArgumentException("groupId required");
            if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
        }
    }

    record CreateTopic(
            String name,
            int numPartitions,
            long retentionBytes,
            long retentionMs
    ) implements Command {
        public CreateTopic {
            if (name == null || name.isBlank()) throw new IllegalArgumentException("name required");
            if (numPartitions < 1) numPartitions = 1;
            if (retentionBytes <= 0) retentionBytes = -1L;
            if (retentionMs <= 0) retentionMs = 7 * 24 * 60 * 60 * 1000L;
        }
    }

    record DeleteTopic(String name) implements Command {
        public DeleteTopic {
            if (name == null || name.isBlank()) throw new IllegalArgumentException("name required");
        }
    }

    record ListTopics() implements Command {}

    record TopicInfo(String topicName) implements Command {
        public TopicInfo {
            if (topicName == null || topicName.isBlank()) throw new IllegalArgumentException("topicName required");
        }
    }

    record Ping(long timestamp) implements Command {}

    // ── Broker → Client ──────────────────────────────────────────────────────

    record Pong(long timestamp) implements Command {}

    record ResponseOk(
            String correlationId,
            Map<String, Object> data
    ) implements Command {
        public ResponseOk {
            if (correlationId == null) correlationId = "";
            if (data == null) data = Map.of();
        }
    }

    record ResponseError(
            String correlationId,
            String errorCode,
            String message
    ) implements Command {
        public ResponseError {
            if (correlationId == null) correlationId = "";
            if (errorCode == null) errorCode = "INTERNAL_ERROR";
            if (message == null) message = "";
        }
    }

    record MessageBatch(
            String topic,
            int partition,
            List<Message> messages
    ) implements Command {
        public MessageBatch {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic required");
            if (messages == null) messages = List.of();
        }
    }
}

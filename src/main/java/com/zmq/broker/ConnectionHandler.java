package com.zmq.broker;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.zmq.message.Message;
import com.zmq.message.Topic;
import com.zmq.protocol.Command;
import com.zmq.protocol.CommandEncoder;
import com.zmq.protocol.CommandParser;
import com.zmq.protocol.ProtocolException;
import com.zmq.storage.MessageLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Handles a single client connection on a virtual thread.
 *
 * Runs a simple read loop: parse command → dispatch → send response.
 * Uses Java 21 pattern matching switch for exhaustive, type-safe dispatch.
 */
public class ConnectionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);
    private static final ObjectMapper MAPPER = buildMapper();

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new StdSerializer<>(Instant.class) {
            @Override
            public void serialize(Instant value, JsonGenerator gen, SerializerProvider p) throws IOException {
                gen.writeNumber(value.toEpochMilli());
            }
        });
        module.addDeserializer(Instant.class, new StdDeserializer<>(Instant.class) {
            @Override
            public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                return Instant.ofEpochMilli(p.getLongValue());
            }
        });
        mapper.registerModule(module);
        return mapper;
    }

    private final String connectionId = UUID.randomUUID().toString();
    private final Socket socket;
    private final TopicManager topicManager;
    private final SubscriptionManager subscriptionManager;

    private volatile Instant lastSeen = Instant.now();
    private final Set<String> activeSubscriptions = new HashSet<>(); // "topic:groupId"

    public ConnectionHandler(Socket socket, TopicManager topicManager,
                              SubscriptionManager subscriptionManager) {
        this.socket = socket;
        this.topicManager = topicManager;
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    public void run() {
        String remote = socket.getRemoteSocketAddress().toString();
        log.debug("Client connected: {} ({})", connectionId, remote);

        try (socket) {
            DataInputStream in   = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            CommandParser  parser  = new CommandParser(in);
            CommandEncoder encoder = new CommandEncoder(out);

            while (!socket.isClosed()) {
                Command cmd;
                try {
                    cmd = parser.readCommand();
                } catch (ProtocolException e) {
                    log.warn("Protocol error from {}: {}", remote, e.getMessage());
                    encoder.write(new Command.ResponseError("", "PROTOCOL_ERROR", e.getMessage()));
                    continue;
                }

                lastSeen = Instant.now();
                Command response = dispatch(cmd);
                if (response != null) {
                    encoder.write(response);
                }
            }
        } catch (EOFException | SocketException ignored) {
            // Client disconnected cleanly or connection reset
        } catch (IOException e) {
            log.debug("Connection {} I/O error: {}", connectionId, e.getMessage());
        } finally {
            cleanup();
            log.debug("Client disconnected: {} ", connectionId);
        }
    }

    public String connectionId() { return connectionId; }
    public Instant lastSeen() { return lastSeen; }

    // ── Command dispatch ─────────────────────────────────────────────────────

    Command dispatch(Command cmd) {
        return switch (cmd) {
            case Command.Publish p       -> handlePublish(p);
            case Command.Subscribe s     -> handleSubscribe(s);
            case Command.Unsubscribe u   -> handleUnsubscribe(u);
            case Command.Fetch f         -> handleFetch(f);
            case Command.Ack a           -> handleAck(a);
            case Command.CreateTopic ct  -> handleCreateTopic(ct);
            case Command.DeleteTopic dt  -> handleDeleteTopic(dt);
            case Command.ListTopics lt   -> handleListTopics(lt);
            case Command.TopicInfo ti    -> handleTopicInfo(ti);
            case Command.Ping ping       -> new Command.Pong(ping.timestamp());
            case Command.Pong ignored    -> { lastSeen = Instant.now(); yield null; }
            case Command.ResponseOk ignored     -> null;
            case Command.ResponseError ignored  -> null;
            case Command.MessageBatch ignored   -> null;
        };
    }

    // ── Handlers ─────────────────────────────────────────────────────────────

    private Command handlePublish(Command.Publish p) {
        try {
            if (!topicManager.topicExists(p.topic())) {
                // Auto-create topic with default 1 partition
                topicManager.createTopic(p.topic(), 1, -1L, 7 * 24 * 60 * 60 * 1000L);
            }
            int partition = p.partition() >= 0
                    ? p.partition()
                    : topicManager.routeToPartition(p.topic(), p.key());

            Message message = new Message(
                    UUID.randomUUID().toString(),
                    p.topic(),
                    partition,
                    -1L,
                    p.payload(),
                    p.headers(),
                    Instant.now(),
                    p.key()
            );

            byte[] bytes = MAPPER.writeValueAsBytes(message);
            long offset = topicManager.append(message, bytes);

            return new Command.ResponseOk("", Map.of(
                    "topic", p.topic(),
                    "partition", partition,
                    "offset", offset
            ));
        } catch (Exception e) {
            log.warn("Publish failed for topic {}: {}", p.topic(), e.getMessage(), e);
            return new Command.ResponseError("", "PUBLISH_FAILED", e.getMessage());
        }
    }

    private Command handleSubscribe(Command.Subscribe s) {
        if (!topicManager.topicExists(s.topic())) {
            return new Command.ResponseError("", "TOPIC_NOT_FOUND", "Topic not found: " + s.topic());
        }
        String key = s.topic() + ":" + s.groupId();
        activeSubscriptions.add(key);
        subscriptionManager.addMember(s.groupId(), connectionId);
        return new Command.ResponseOk("", Map.of("topic", s.topic(), "groupId", s.groupId()));
    }

    private Command handleUnsubscribe(Command.Unsubscribe u) {
        String key = u.topic() + ":" + u.groupId();
        activeSubscriptions.remove(key);
        subscriptionManager.removeMember(u.groupId(), connectionId);
        return new Command.ResponseOk("", Map.of());
    }

    private Command handleFetch(Command.Fetch f) {
        if (!topicManager.topicExists(f.topic())) {
            return new Command.ResponseError("", "TOPIC_NOT_FOUND", "Topic not found: " + f.topic());
        }

        try {
            long fromOffset = f.fromOffset();
            if (fromOffset < 0) {
                // Use committed offset, or 0 if none
                long committed = subscriptionManager.getCommittedOffset(f.groupId(), f.topic(), f.partition());
                fromOffset = committed >= 0 ? committed + 1 : 0L;
            }

            MessageLog ml = topicManager.getLog(f.topic(), f.partition());
            List<byte[]> rawRecords = ml.read(fromOffset, f.maxMessages());

            // Long-poll: wait for new messages if none available
            if (rawRecords.isEmpty() && f.maxWaitMs() > 0) {
                boolean newData = topicManager.awaitNewMessages(f.topic(), f.partition(), f.maxWaitMs());
                if (newData) {
                    rawRecords = ml.read(fromOffset, f.maxMessages());
                }
            }

            List<Message> messages = new ArrayList<>(rawRecords.size());
            for (int i = 0; i < rawRecords.size(); i++) {
                Message msg = MAPPER.readValue(rawRecords.get(i), Message.class);
                // Assign the correct logical offset based on position in log
                messages.add(msg.withOffset(fromOffset + i));
            }

            return new Command.MessageBatch(f.topic(), f.partition(), messages);
        } catch (Exception e) {
            log.warn("Fetch failed for {}/{}: {}", f.topic(), f.partition(), e.getMessage(), e);
            return new Command.ResponseError("", "FETCH_FAILED", e.getMessage());
        }
    }

    private Command handleAck(Command.Ack a) {
        if (!topicManager.topicExists(a.topic())) {
            return new Command.ResponseError("", "TOPIC_NOT_FOUND", "Topic not found: " + a.topic());
        }
        subscriptionManager.commitOffset(a.groupId(), a.topic(), a.partition(), a.offset());
        return new Command.ResponseOk("", Map.of(
                "topic", a.topic(),
                "partition", a.partition(),
                "groupId", a.groupId(),
                "offset", a.offset()
        ));
    }

    private Command handleCreateTopic(Command.CreateTopic ct) {
        try {
            Topic topic = topicManager.createTopic(
                    ct.name(), ct.numPartitions(), ct.retentionBytes(), ct.retentionMs());
            return new Command.ResponseOk("", Map.of(
                    "name", topic.name(),
                    "numPartitions", topic.numPartitions()
            ));
        } catch (Exception e) {
            return new Command.ResponseError("", "CREATE_TOPIC_FAILED", e.getMessage());
        }
    }

    private Command handleDeleteTopic(Command.DeleteTopic dt) {
        try {
            topicManager.deleteTopic(dt.name());
            return new Command.ResponseOk("", Map.of("name", dt.name()));
        } catch (IllegalArgumentException e) {
            return new Command.ResponseError("", "TOPIC_NOT_FOUND", e.getMessage());
        } catch (Exception e) {
            return new Command.ResponseError("", "DELETE_TOPIC_FAILED", e.getMessage());
        }
    }

    private Command handleListTopics(Command.ListTopics ignored) {
        List<String> names = topicManager.listTopics().stream()
                .map(Topic::name).toList();
        return new Command.ResponseOk("", Map.of("topics", names));
    }

    private Command handleTopicInfo(Command.TopicInfo ti) {
        Topic topic = topicManager.getTopic(ti.topicName());
        if (topic == null) {
            return new Command.ResponseError("", "TOPIC_NOT_FOUND", "Topic not found: " + ti.topicName());
        }
        Map<String, Object> data = new HashMap<>();
        data.put("name", topic.name());
        data.put("numPartitions", topic.numPartitions());
        data.put("createdAt", topic.createdAt().toString());
        // partition offsets
        Map<String, Long> offsets = new HashMap<>();
        for (int p = 0; p < topic.numPartitions(); p++) {
            try {
                offsets.put(String.valueOf(p), topicManager.getLog(topic.name(), p).nextOffset());
            } catch (Exception ignored) {
                offsets.put(String.valueOf(p), -1L);
            }
        }
        data.put("partitionOffsets", offsets);
        return new Command.ResponseOk("", data);
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    private void cleanup() {
        for (String sub : activeSubscriptions) {
            String[] parts = sub.split(":", 2);
            if (parts.length == 2) {
                subscriptionManager.removeMember(parts[1], connectionId);
            }
        }
        activeSubscriptions.clear();
    }
}

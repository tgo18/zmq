package com.zmq.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.zmq.message.Message;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encodes a {@link Command} into wire frames and writes them to a {@link DataOutputStream}.
 *
 * Thread-safety: NOT thread-safe — external synchronization required if multiple threads
 * share the same CommandEncoder (one per connection, so this is normally fine).
 */
public class CommandEncoder {

    private static final ObjectMapper MAPPER = buildMapper();

    private final DataOutputStream out;

    public CommandEncoder(DataOutputStream out) {
        this.out = out;
    }

    /**
     * Encode and write a command to the output stream.
     * Writes: [MAGIC(2)] [TYPE(1)] [LENGTH(4)] [PAYLOAD(n)]
     */
    public void write(Command command) throws IOException {
        if (command == null) return;

        byte type = typeOf(command);
        byte[] payload = MAPPER.writeValueAsBytes(toMap(command));

        if (payload.length > Frame.MAX_PAYLOAD_SIZE) {
            throw new IOException("Payload size " + payload.length + " exceeds limit " + Frame.MAX_PAYLOAD_SIZE);
        }

        synchronized (out) {
            out.writeShort(Frame.MAGIC);
            out.writeByte(type);
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
        }
    }

    // ── Type byte mapping ────────────────────────────────────────────────────

    static byte typeOf(Command command) {
        return switch (command) {
            case Command.Publish        ignored -> Frame.Type.PUBLISH;
            case Command.Subscribe      ignored -> Frame.Type.SUBSCRIBE;
            case Command.Unsubscribe    ignored -> Frame.Type.UNSUBSCRIBE;
            case Command.Fetch          ignored -> Frame.Type.FETCH;
            case Command.Ack            ignored -> Frame.Type.ACK;
            case Command.CreateTopic    ignored -> Frame.Type.CREATE_TOPIC;
            case Command.DeleteTopic    ignored -> Frame.Type.DELETE_TOPIC;
            case Command.ListTopics     ignored -> Frame.Type.LIST_TOPICS;
            case Command.TopicInfo      ignored -> Frame.Type.TOPIC_INFO;
            case Command.Ping           ignored -> Frame.Type.PING;
            case Command.Pong           ignored -> Frame.Type.PONG;
            case Command.ResponseOk     ignored -> Frame.Type.RESPONSE_OK;
            case Command.ResponseError  ignored -> Frame.Type.RESPONSE_ERROR;
            case Command.MessageBatch   ignored -> Frame.Type.MESSAGE_BATCH;
        };
    }

    // ── Command → Map serialization ──────────────────────────────────────────

    @SuppressWarnings("unchecked")
    static Map<String, Object> toMap(Command command) {
        return switch (command) {
            case Command.Publish p -> Map.of(
                    "topic", p.topic(),
                    "partition", p.partition(),
                    "key", p.key() != null ? p.key() : "",
                    "payload", Base64.getEncoder().encodeToString(p.payload()),
                    "headers", p.headers(),
                    "produceTimeoutMs", p.produceTimeoutMs()
            );
            case Command.Subscribe s -> Map.of(
                    "topic", s.topic(),
                    "partition", s.partition(),
                    "groupId", s.groupId()
            );
            case Command.Unsubscribe u -> Map.of(
                    "topic", u.topic(),
                    "groupId", u.groupId()
            );
            case Command.Fetch f -> Map.of(
                    "topic", f.topic(),
                    "partition", f.partition(),
                    "groupId", f.groupId(),
                    "fromOffset", f.fromOffset(),
                    "maxMessages", f.maxMessages(),
                    "maxWaitMs", f.maxWaitMs()
            );
            case Command.Ack a -> Map.of(
                    "topic", a.topic(),
                    "partition", a.partition(),
                    "groupId", a.groupId(),
                    "offset", a.offset()
            );
            case Command.CreateTopic ct -> Map.of(
                    "name", ct.name(),
                    "numPartitions", ct.numPartitions(),
                    "retentionBytes", ct.retentionBytes(),
                    "retentionMs", ct.retentionMs()
            );
            case Command.DeleteTopic dt -> Map.of("name", dt.name());
            case Command.ListTopics    ignored -> Map.of();
            case Command.TopicInfo ti  -> Map.of("topicName", ti.topicName());
            case Command.Ping p        -> Map.of("timestamp", p.timestamp());
            case Command.Pong p        -> Map.of("timestamp", p.timestamp());
            case Command.ResponseOk ok -> {
                Map<String, Object> m = new HashMap<>();
                m.put("correlationId", ok.correlationId());
                m.put("data", ok.data());
                yield m;
            }
            case Command.ResponseError err -> Map.of(
                    "correlationId", err.correlationId(),
                    "errorCode", err.errorCode(),
                    "message", err.message()
            );
            case Command.MessageBatch mb -> Map.of(
                    "topic", mb.topic(),
                    "partition", mb.partition(),
                    "messages", mb.messages().stream()
                            .map(CommandEncoder::messageToMap)
                            .collect(Collectors.toList())
            );
        };
    }

    static Map<String, Object> messageToMap(Message m) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", m.id());
        map.put("topic", m.topic());
        map.put("partition", m.partition());
        map.put("offset", m.offset());
        map.put("payload", Base64.getEncoder().encodeToString(m.payload()));
        map.put("headers", m.headers());
        map.put("timestamp", m.timestamp().toEpochMilli());
        map.put("key", m.key() != null ? m.key() : "");
        return map;
    }

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}

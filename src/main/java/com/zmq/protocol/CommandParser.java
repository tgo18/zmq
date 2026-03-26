package com.zmq.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zmq.message.Message;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Reads and decodes {@link Command} objects from a {@link DataInputStream}.
 *
 * Blocking: each {@code readCommand()} call blocks until a full frame is available.
 * Designed to run on a virtual thread where blocking is cheap.
 */
public class CommandParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataInputStream in;

    public CommandParser(DataInputStream in) {
        this.in = in;
    }

    /**
     * Block until a full frame is available, then decode and return the Command.
     *
     * @throws EOFException      if the connection closed cleanly
     * @throws ProtocolException if the frame is malformed
     * @throws IOException       for I/O errors
     */
    public Command readCommand() throws IOException, ProtocolException {
        // 1. Read magic bytes
        short magic = in.readShort();
        if (magic != Frame.MAGIC) {
            throw new ProtocolException(
                    String.format("Invalid magic bytes: 0x%04X (expected 0x%04X)", magic & 0xFFFF, Frame.MAGIC & 0xFFFF));
        }

        // 2. Read type byte
        byte type = in.readByte();

        // 3. Read payload length
        int length = in.readInt();
        if (length < 0 || length > Frame.MAX_PAYLOAD_SIZE) {
            throw new ProtocolException("Invalid payload length: " + length);
        }

        // 4. Read payload bytes
        byte[] payload = new byte[length];
        in.readFully(payload);

        // 5. Decode payload to Command
        return decode(type, payload);
    }

    // ── Decode ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Command decode(byte type, byte[] payload) throws IOException, ProtocolException {
        Map<String, Object> map = payload.length == 0
                ? Map.of()
                : MAPPER.readValue(payload, Map.class);

        return switch (type) {
            case Frame.Type.PUBLISH -> new Command.Publish(
                    str(map, "topic"),
                    intVal(map, "partition"),
                    strOrNull(map, "key"),
                    decodeBytes(map, "payload"),
                    (Map<String, String>) map.getOrDefault("headers", Map.of()),
                    longVal(map, "produceTimeoutMs", 5000L)
            );
            case Frame.Type.SUBSCRIBE -> new Command.Subscribe(
                    str(map, "topic"),
                    intVal(map, "partition"),
                    str(map, "groupId")
            );
            case Frame.Type.UNSUBSCRIBE -> new Command.Unsubscribe(
                    str(map, "topic"),
                    str(map, "groupId")
            );
            case Frame.Type.FETCH -> new Command.Fetch(
                    str(map, "topic"),
                    intVal(map, "partition"),
                    str(map, "groupId"),
                    longVal(map, "fromOffset", -1L),
                    intVal(map, "maxMessages", 100),
                    longVal(map, "maxWaitMs", 0L)
            );
            case Frame.Type.ACK -> new Command.Ack(
                    str(map, "topic"),
                    intVal(map, "partition"),
                    str(map, "groupId"),
                    longVal(map, "offset", 0L)
            );
            case Frame.Type.CREATE_TOPIC -> new Command.CreateTopic(
                    str(map, "name"),
                    intVal(map, "numPartitions", 1),
                    longVal(map, "retentionBytes", -1L),
                    longVal(map, "retentionMs", 7 * 24 * 60 * 60 * 1000L)
            );
            case Frame.Type.DELETE_TOPIC -> new Command.DeleteTopic(str(map, "name"));
            case Frame.Type.LIST_TOPICS  -> new Command.ListTopics();
            case Frame.Type.TOPIC_INFO   -> new Command.TopicInfo(str(map, "topicName"));
            case Frame.Type.PING         -> new Command.Ping(longVal(map, "timestamp", 0L));
            case Frame.Type.PONG         -> new Command.Pong(longVal(map, "timestamp", 0L));
            case Frame.Type.RESPONSE_OK  -> new Command.ResponseOk(
                    strOrNull(map, "correlationId"),
                    (Map<String, Object>) map.getOrDefault("data", Map.of())
            );
            case Frame.Type.RESPONSE_ERROR -> new Command.ResponseError(
                    strOrNull(map, "correlationId"),
                    strOrNull(map, "errorCode"),
                    strOrNull(map, "message")
            );
            case Frame.Type.MESSAGE_BATCH -> {
                String topic = str(map, "topic");
                int partition = intVal(map, "partition");
                List<?> rawMessages = (List<?>) map.getOrDefault("messages", List.of());
                List<Message> messages = new ArrayList<>(rawMessages.size());
                for (Object raw : rawMessages) {
                    messages.add(decodeMessage((Map<String, Object>) raw));
                }
                yield new Command.MessageBatch(topic, partition, messages);
            }
            default -> throw new ProtocolException("Unknown command type: 0x" + String.format("%02X", type));
        };
    }

    @SuppressWarnings("unchecked")
    private Message decodeMessage(Map<String, Object> m) throws ProtocolException {
        return new Message(
                str(m, "id"),
                str(m, "topic"),
                intVal(m, "partition"),
                longVal(m, "offset", 0L),
                decodeBytes(m, "payload"),
                (Map<String, String>) m.getOrDefault("headers", Map.of()),
                Instant.ofEpochMilli(longVal(m, "timestamp", 0L)),
                strOrNull(m, "key")
        );
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String str(Map<String, Object> map, String key) throws ProtocolException {
        Object v = map.get(key);
        if (v == null) throw new ProtocolException("Missing required field: " + key);
        return v.toString();
    }

    private static String strOrNull(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v != null ? v.toString() : null;
    }

    private static int intVal(Map<String, Object> map, String key) throws ProtocolException {
        Object v = map.get(key);
        if (v == null) throw new ProtocolException("Missing required field: " + key);
        return ((Number) v).intValue();
    }

    private static int intVal(Map<String, Object> map, String key, int defaultVal) {
        Object v = map.get(key);
        return v != null ? ((Number) v).intValue() : defaultVal;
    }

    private static long longVal(Map<String, Object> map, String key, long defaultVal) {
        Object v = map.get(key);
        return v != null ? ((Number) v).longValue() : defaultVal;
    }

    private static byte[] decodeBytes(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v == null || v.toString().isEmpty()) return new byte[0];
        return Base64.getDecoder().decode(v.toString());
    }
}

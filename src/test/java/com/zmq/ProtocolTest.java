package com.zmq;

import com.zmq.message.Message;
import com.zmq.protocol.Command;
import com.zmq.protocol.CommandEncoder;
import com.zmq.protocol.CommandParser;
import com.zmq.protocol.Frame;
import com.zmq.protocol.ProtocolException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Protocol layer round-trip tests — no sockets needed.
 */
class ProtocolTest {

    private Command roundTrip(Command cmd) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CommandEncoder encoder = new CommandEncoder(new DataOutputStream(baos));
        encoder.write(cmd);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        CommandParser parser = new CommandParser(new DataInputStream(bais));
        return parser.readCommand();
    }

    @Test
    void testPublishRoundTrip() throws Exception {
        Command.Publish original = new Command.Publish(
                "orders", 0, "key-1", "hello".getBytes(), Map.of("x", "y"), 5000L);
        Command.Publish decoded = (Command.Publish) roundTrip(original);

        assertEquals("orders", decoded.topic());
        assertEquals(0, decoded.partition());
        assertEquals("key-1", decoded.key());
        assertArrayEquals("hello".getBytes(), decoded.payload());
        assertEquals(Map.of("x", "y"), decoded.headers());
        assertEquals(5000L, decoded.produceTimeoutMs());
    }

    @Test
    void testSubscribeRoundTrip() throws Exception {
        Command.Subscribe original = new Command.Subscribe("events", 1, "billing");
        Command.Subscribe decoded = (Command.Subscribe) roundTrip(original);

        assertEquals("events", decoded.topic());
        assertEquals(1, decoded.partition());
        assertEquals("billing", decoded.groupId());
    }

    @Test
    void testFetchRoundTrip() throws Exception {
        Command.Fetch original = new Command.Fetch("logs", 0, "analytics", 42L, 50, 1000L);
        Command.Fetch decoded = (Command.Fetch) roundTrip(original);

        assertEquals("logs", decoded.topic());
        assertEquals(0, decoded.partition());
        assertEquals("analytics", decoded.groupId());
        assertEquals(42L, decoded.fromOffset());
        assertEquals(50, decoded.maxMessages());
        assertEquals(1000L, decoded.maxWaitMs());
    }

    @Test
    void testAckRoundTrip() throws Exception {
        Command.Ack original = new Command.Ack("payments", 2, "risk-service", 99L);
        Command.Ack decoded = (Command.Ack) roundTrip(original);

        assertEquals("payments", decoded.topic());
        assertEquals(2, decoded.partition());
        assertEquals("risk-service", decoded.groupId());
        assertEquals(99L, decoded.offset());
    }

    @Test
    void testCreateTopicRoundTrip() throws Exception {
        Command.CreateTopic original = new Command.CreateTopic("orders", 4, 1024L * 1024, 86400000L);
        Command.CreateTopic decoded = (Command.CreateTopic) roundTrip(original);

        assertEquals("orders", decoded.name());
        assertEquals(4, decoded.numPartitions());
        assertEquals(1024L * 1024, decoded.retentionBytes());
        assertEquals(86400000L, decoded.retentionMs());
    }

    @Test
    void testDeleteTopicRoundTrip() throws Exception {
        Command.DeleteTopic original = new Command.DeleteTopic("old-topic");
        Command.DeleteTopic decoded = (Command.DeleteTopic) roundTrip(original);
        assertEquals("old-topic", decoded.name());
    }

    @Test
    void testPingPongRoundTrip() throws Exception {
        long ts = System.currentTimeMillis();
        Command.Ping ping = new Command.Ping(ts);
        Command.Ping decodedPing = (Command.Ping) roundTrip(ping);
        assertEquals(ts, decodedPing.timestamp());

        Command.Pong pong = new Command.Pong(ts);
        Command.Pong decodedPong = (Command.Pong) roundTrip(pong);
        assertEquals(ts, decodedPong.timestamp());
    }

    @Test
    void testResponseOkRoundTrip() throws Exception {
        Command.ResponseOk original = new Command.ResponseOk("corr-1",
                Map.of("offset", 42L, "partition", 0));
        Command.ResponseOk decoded = (Command.ResponseOk) roundTrip(original);

        assertEquals("corr-1", decoded.correlationId());
        assertEquals(42L, ((Number) decoded.data().get("offset")).longValue());
    }

    @Test
    void testResponseErrorRoundTrip() throws Exception {
        Command.ResponseError original = new Command.ResponseError("corr-2", "NOT_FOUND", "Topic missing");
        Command.ResponseError decoded = (Command.ResponseError) roundTrip(original);

        assertEquals("corr-2", decoded.correlationId());
        assertEquals("NOT_FOUND", decoded.errorCode());
        assertEquals("Topic missing", decoded.message());
    }

    @Test
    void testMessageBatchRoundTrip() throws Exception {
        Message msg = new Message("id-1", "orders", 0, 5L,
                "payload".getBytes(), Map.of("k", "v"), Instant.ofEpochMilli(1000L), "key-a");
        Command.MessageBatch original = new Command.MessageBatch("orders", 0, List.of(msg));
        Command.MessageBatch decoded = (Command.MessageBatch) roundTrip(original);

        assertEquals("orders", decoded.topic());
        assertEquals(0, decoded.partition());
        assertEquals(1, decoded.messages().size());
        Message dm = decoded.messages().getFirst();
        assertEquals("id-1", dm.id());
        assertEquals(5L, dm.offset());
        assertArrayEquals("payload".getBytes(), dm.payload());
    }

    @Test
    void testListTopicsRoundTrip() throws Exception {
        Command.ListTopics decoded = (Command.ListTopics) roundTrip(new Command.ListTopics());
        assertNotNull(decoded);
    }

    @Test
    void testTopicInfoRoundTrip() throws Exception {
        Command.TopicInfo decoded = (Command.TopicInfo) roundTrip(new Command.TopicInfo("my-topic"));
        assertEquals("my-topic", decoded.topicName());
    }

    @Test
    void testInvalidMagicThrows() {
        byte[] badFrame = {0x00, 0x00, Frame.Type.PING, 0, 0, 0, 0};
        ByteArrayInputStream bais = new ByteArrayInputStream(badFrame);
        CommandParser parser = new CommandParser(new DataInputStream(bais));
        assertThrows(ProtocolException.class, parser::readCommand);
    }

    @Test
    void testEmptyPayloadPublish() throws Exception {
        Command.Publish original = new Command.Publish("t", 0, null, new byte[0], Map.of(), 5000L);
        Command.Publish decoded = (Command.Publish) roundTrip(original);
        assertArrayEquals(new byte[0], decoded.payload());
    }

    @Test
    void testMultipleFramesInSequence() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CommandEncoder encoder = new CommandEncoder(new DataOutputStream(baos));
        encoder.write(new Command.Ping(1L));
        encoder.write(new Command.Pong(2L));
        encoder.write(new Command.ListTopics());

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        CommandParser parser = new CommandParser(new DataInputStream(bais));

        assertTrue(parser.readCommand() instanceof Command.Ping);
        assertTrue(parser.readCommand() instanceof Command.Pong);
        assertTrue(parser.readCommand() instanceof Command.ListTopics);
    }
}

package com.zmq;

import com.zmq.storage.MessageLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Storage layer tests — uses @TempDir for isolated file system state.
 */
class MessageLogTest {

    @TempDir
    Path tempDir;

    @Test
    void testAppendAndRead() throws Exception {
        MessageLog log = new MessageLog(tempDir, 10 * 1024 * 1024L);

        byte[] data1 = "hello".getBytes();
        byte[] data2 = "world".getBytes();

        long offset1 = log.append(data1);
        long offset2 = log.append(data2);

        assertEquals(0L, offset1);
        assertEquals(1L, offset2);
        assertEquals(2L, log.nextOffset());

        List<byte[]> records = log.read(0, 10);
        assertEquals(2, records.size());
        assertArrayEquals(data1, records.get(0));
        assertArrayEquals(data2, records.get(1));

        log.close();
    }

    @Test
    void testReadFromMiddle() throws Exception {
        MessageLog log = new MessageLog(tempDir, 10 * 1024 * 1024L);

        for (int i = 0; i < 10; i++) {
            log.append(("msg-" + i).getBytes());
        }

        List<byte[]> records = log.read(5, 3);
        assertEquals(3, records.size());
        assertArrayEquals("msg-5".getBytes(), records.get(0));
        assertArrayEquals("msg-6".getBytes(), records.get(1));
        assertArrayEquals("msg-7".getBytes(), records.get(2));

        log.close();
    }

    @Test
    void testReadBeyondEndReturnsEmpty() throws Exception {
        MessageLog log = new MessageLog(tempDir, 10 * 1024 * 1024L);
        log.append("hello".getBytes());
        List<byte[]> records = log.read(100, 10);
        assertTrue(records.isEmpty());
        log.close();
    }

    @Test
    void testSegmentRolling() throws Exception {
        // Small segment size to force rolling
        int maxSegment = 200;
        MessageLog log = new MessageLog(tempDir, maxSegment);

        // Append enough data to trigger at least one segment roll
        for (int i = 0; i < 20; i++) {
            log.append(("message-number-" + String.format("%03d", i)).getBytes());
        }

        // Verify multiple segment files were created
        long fileCount = java.nio.file.Files.list(tempDir)
                .filter(p -> p.getFileName().toString().endsWith(".log"))
                .count();
        assertTrue(fileCount > 1, "Expected multiple segments, got " + fileCount);

        // Still readable from offset 0
        List<byte[]> all = log.read(0, 100);
        assertEquals(20, all.size());

        log.close();
    }

    @Test
    void testCrashRecovery() throws Exception {
        // Write 5 messages, close, reopen, verify all readable
        {
            MessageLog log = new MessageLog(tempDir, 10 * 1024 * 1024L);
            for (int i = 0; i < 5; i++) {
                log.append(("recover-" + i).getBytes());
            }
            log.flush();
            log.close();
        }

        // Reopen
        {
            MessageLog log = new MessageLog(tempDir, 10 * 1024 * 1024L);
            assertEquals(5L, log.nextOffset());

            List<byte[]> records = log.read(0, 10);
            assertEquals(5, records.size());
            assertArrayEquals("recover-0".getBytes(), records.get(0));
            assertArrayEquals("recover-4".getBytes(), records.get(4));

            // Can append more
            long offset = log.append("after-recovery".getBytes());
            assertEquals(5L, offset);

            log.close();
        }
    }

    @Test
    void testRetentionBySize() throws Exception {
        MessageLog log = new MessageLog(tempDir, 200L);

        for (int i = 0; i < 20; i++) {
            log.append(("msg" + i).getBytes());
        }

        long sizeBefore = java.nio.file.Files.list(tempDir)
                .filter(p -> p.getFileName().toString().endsWith(".log"))
                .count();
        assertTrue(sizeBefore > 1);

        // Apply retention: keep only 200 bytes total
        log.applyRetention(null, 200L);

        log.close();
    }

    @Test
    void testLargeNumberOfMessages() throws Exception {
        MessageLog log = new MessageLog(tempDir, 1024 * 1024L);

        int count = 1000;
        for (int i = 0; i < count; i++) {
            long offset = log.append(("item-" + i).getBytes());
            assertEquals(i, offset);
        }
        assertEquals(count, log.nextOffset());

        // Read last 10
        List<byte[]> tail = log.read(990, 10);
        assertEquals(10, tail.size());
        assertArrayEquals("item-990".getBytes(), tail.getFirst());
        assertArrayEquals("item-999".getBytes(), tail.getLast());

        log.close();
    }

    @Test
    void testEmptyLogRead() throws Exception {
        MessageLog log = new MessageLog(tempDir, 10 * 1024 * 1024L);
        assertEquals(0L, log.nextOffset());
        List<byte[]> records = log.read(0, 10);
        assertTrue(records.isEmpty());
        log.close();
    }
}

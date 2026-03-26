package com.zmq.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Sparse offset-to-position index for a log segment.
 *
 * Binary file of fixed-size 16-byte entries:
 *   [8 bytes: logical offset][8 bytes: physical file position]
 *
 * Usage: one entry is written for every {@code INDEX_INTERVAL_BYTES} of log data.
 * On lookup, binary search finds the largest indexed offset <= target,
 * then the caller scans forward from that position.
 */
public class OffsetIndex implements AutoCloseable {

    static final int ENTRY_SIZE = 16; // 8 + 8

    private final FileChannel channel;
    private int entryCount;

    public OffsetIndex(Path indexPath) throws IOException {
        this.channel = FileChannel.open(indexPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.entryCount = (int) (channel.size() / ENTRY_SIZE);
    }

    /**
     * Append a new index entry (offset → position).
     * Entries must be appended in monotonically increasing offset order.
     */
    public synchronized void append(long offset, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putLong(offset);
        buf.putLong(position);
        buf.flip();
        channel.write(buf, (long) entryCount * ENTRY_SIZE);
        entryCount++;
    }

    /**
     * Find the largest indexed offset <= targetOffset.
     *
     * @return the physical file position for that offset, or 0 if no entry found.
     */
    public synchronized long lookup(long targetOffset) throws IOException {
        if (entryCount == 0) return 0L;

        int lo = 0, hi = entryCount - 1;
        long bestPosition = 0L;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long indexedOffset = readOffset(mid);

            if (indexedOffset <= targetOffset) {
                bestPosition = readPosition(mid);
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return bestPosition;
    }

    public synchronized int entryCount() {
        return entryCount;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private long readOffset(int entryIndex) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        channel.read(buf, (long) entryIndex * ENTRY_SIZE);
        buf.flip();
        return buf.getLong();
    }

    private long readPosition(int entryIndex) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        channel.read(buf, (long) entryIndex * ENTRY_SIZE + 8);
        buf.flip();
        return buf.getLong();
    }
}

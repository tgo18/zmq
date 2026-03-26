package com.zmq.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * A single log segment — one {@code .log} file paired with one {@code .index} file.
 *
 * Log file format (binary, per record):
 *   [4 bytes: record length][4 bytes: CRC32][record-length bytes: message data]
 *
 * Appends are sequential. Reads require a physical file position from {@link OffsetIndex}.
 *
 * The segment is named by its base offset (first offset it contains).
 * File names are zero-padded to 20 digits, e.g. {@code 00000000000000000000.log}.
 */
public class LogSegment implements AutoCloseable {

    /** Bytes of log data between index entries (sparse index). */
    static final int INDEX_INTERVAL_BYTES = 4096;

    private static final int RECORD_HEADER_SIZE = 8; // 4 (length) + 4 (CRC32)

    private final long baseOffset;
    private final FileChannel logChannel;
    private final OffsetIndex index;

    private long nextOffset;     // next offset to assign
    private long sizeBytes;      // current .log file size in bytes
    private long bytesSinceLastIndex = 0; // for sparse index control

    /**
     * Open or create a segment with the given base offset.
     *
     * @param logPath   path to the {@code .log} file
     * @param indexPath path to the {@code .index} file
     * @param baseOffset first logical offset of this segment
     */
    public LogSegment(Path logPath, Path indexPath, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.logChannel = FileChannel.open(logPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        this.index = new OffsetIndex(indexPath);
        this.sizeBytes = logChannel.size();

        // Recover nextOffset by scanning to the end of the log
        this.nextOffset = baseOffset + recoverRecordCount();
    }

    /** Append raw message bytes. Returns the logical offset assigned. */
    public synchronized long append(byte[] data) throws IOException {
        long offset = nextOffset;

        // Record header: length (4) + CRC32 (4)
        int length = data.length;
        int crc = computeCrc(data);

        ByteBuffer buf = ByteBuffer.allocate(RECORD_HEADER_SIZE + length);
        buf.putInt(length);
        buf.putInt(crc);
        buf.put(data);
        buf.flip();

        long position = sizeBytes;
        logChannel.write(buf, position);
        sizeBytes += RECORD_HEADER_SIZE + length;
        nextOffset++;

        // Append to sparse index if we've crossed the interval
        bytesSinceLastIndex += RECORD_HEADER_SIZE + length;
        if (bytesSinceLastIndex >= INDEX_INTERVAL_BYTES || offset == baseOffset) {
            index.append(offset, position);
            bytesSinceLastIndex = 0;
        }

        return offset;
    }

    /**
     * Read up to {@code maxCount} raw message byte arrays starting from the
     * given physical {@code position} in the log file.
     */
    public synchronized List<byte[]> readFrom(long position, int maxCount) throws IOException {
        List<byte[]> results = new ArrayList<>(Math.min(maxCount, 64));
        long pos = position;
        long fileSize = sizeBytes;

        while (results.size() < maxCount && pos < fileSize) {
            // Read record length
            ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = logChannel.read(header, pos);
            if (read < RECORD_HEADER_SIZE) break;
            header.flip();

            int length = header.getInt();
            int storedCrc = header.getInt();

            if (length < 0 || pos + RECORD_HEADER_SIZE + length > fileSize) break;

            // Read record data
            ByteBuffer dataBuf = ByteBuffer.allocate(length);
            logChannel.read(dataBuf, pos + RECORD_HEADER_SIZE);
            dataBuf.flip();
            byte[] data = dataBuf.array();

            // Verify CRC
            int actualCrc = computeCrc(data);
            if (actualCrc != storedCrc) {
                throw new IOException(String.format(
                        "CRC mismatch at position %d: stored=%08X actual=%08X", pos, storedCrc, actualCrc));
            }

            results.add(data);
            pos += RECORD_HEADER_SIZE + length;
        }
        return results;
    }

    /**
     * Translate a logical offset to a physical file position using the sparse index.
     * Then scan forward to find the exact record.
     *
     * @return the physical position of the record with the given offset,
     *         or -1 if not found in this segment.
     */
    public synchronized long positionForOffset(long targetOffset) throws IOException {
        if (targetOffset < baseOffset || targetOffset >= nextOffset) return -1L;

        long pos = index.lookup(targetOffset);
        long startOffset = baseOffset; // conservative: scan from beginning if index returns 0

        // Find the offset of the record at pos by counting records from baseOffset
        // More efficient: walk forward from the index entry position
        long currentOffset = offsetAtPosition(pos);
        if (currentOffset < 0) currentOffset = baseOffset;

        // Scan forward from pos until we reach targetOffset
        while (currentOffset < targetOffset && pos < sizeBytes) {
            ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = logChannel.read(header, pos);
            if (read < RECORD_HEADER_SIZE) break;
            header.flip();
            int length = header.getInt();
            pos += RECORD_HEADER_SIZE + length;
            currentOffset++;
        }
        return (currentOffset == targetOffset) ? pos : -1L;
    }

    public long baseOffset() { return baseOffset; }
    public long nextOffset() { return nextOffset; }
    public long sizeBytes() { return sizeBytes; }

    public void flush() throws IOException {
        logChannel.force(false);
    }

    @Override
    public void close() throws IOException {
        try {
            logChannel.force(false);
        } finally {
            logChannel.close();
            index.close();
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private long recoverRecordCount() throws IOException {
        long pos = 0;
        long count = 0;
        long fileSize = logChannel.size();

        while (pos < fileSize) {
            ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = logChannel.read(header, pos);
            if (read < RECORD_HEADER_SIZE) break;
            header.flip();
            int length = header.getInt();
            if (length < 0 || pos + RECORD_HEADER_SIZE + length > fileSize) break;
            pos += RECORD_HEADER_SIZE + length;
            count++;
        }
        return count;
    }

    /**
     * Determine the logical offset of the record at the given physical position
     * by counting records from the segment base.
     */
    private long offsetAtPosition(long targetPos) throws IOException {
        long pos = 0;
        long offset = baseOffset;
        while (pos < targetPos && pos < sizeBytes) {
            ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = logChannel.read(header, pos);
            if (read < RECORD_HEADER_SIZE) break;
            header.flip();
            int length = header.getInt();
            pos += RECORD_HEADER_SIZE + length;
            offset++;
        }
        return (pos == targetPos) ? offset : -1L;
    }

    private static int computeCrc(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }
}

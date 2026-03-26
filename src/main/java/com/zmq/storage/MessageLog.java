package com.zmq.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Manages the full log for a single (topic, partition) pair.
 *
 * Responsibilities:
 * - Creates and rolls log segments when the active one exceeds {@code maxSegmentBytes}.
 * - Maintains a sorted list of segments; the last element is the active (writable) one.
 * - Enforces retention policies (by age or total size).
 * - Thread-safe via a {@link ReentrantReadWriteLock} (many readers, one writer).
 *
 * Directory structure:
 * <pre>
 *   &lt;logDir&gt;/
 *     00000000000000000000.log
 *     00000000000000000000.index
 *     00000000000000001024.log
 *     00000000000000001024.index
 *     ...
 * </pre>
 */
public class MessageLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageLog.class);

    private final Path logDir;
    private final long maxSegmentBytes;
    private final List<LogSegment> segments; // sorted by base offset, last = active
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MessageLog(Path logDir, long maxSegmentBytes) throws IOException {
        this.logDir = logDir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.segments = new ArrayList<>();
        Files.createDirectories(logDir);
        loadOrCreateSegments();
    }

    /**
     * Append raw bytes and return the assigned logical offset.
     */
    public long append(byte[] data) throws IOException {
        lock.writeLock().lock();
        try {
            LogSegment active = activeSegment();
            if (active.sizeBytes() >= maxSegmentBytes) {
                active.flush();
                active = rollSegment(active.nextOffset());
            }
            return active.append(data);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Read up to {@code maxCount} raw byte records starting at logical {@code fromOffset}.
     * Returns an empty list if {@code fromOffset} is beyond the end of the log.
     */
    public List<byte[]> read(long fromOffset, int maxCount) throws IOException {
        lock.readLock().lock();
        try {
            if (segments.isEmpty()) return List.of();

            int segIdx = findSegmentIndexForOffset(fromOffset);
            if (segIdx < 0) return List.of();

            List<byte[]> results = new ArrayList<>();
            for (int i = segIdx; i < segments.size() && results.size() < maxCount; i++) {
                LogSegment segment = segments.get(i);
                long startOffset = (i == segIdx) ? fromOffset : segment.baseOffset();
                long physPos = segment.positionForOffset(startOffset);
                if (physPos < 0) continue;
                List<byte[]> batch = segment.readFrom(physPos, maxCount - results.size());
                results.addAll(batch);
            }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the offset that the next appended message will receive.
     */
    public long nextOffset() {
        lock.readLock().lock();
        try {
            return segments.isEmpty() ? 0L : activeSegment().nextOffset();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Force all unflushed data in the active segment to disk.
     */
    public void flush() throws IOException {
        lock.readLock().lock();
        try {
            if (!segments.isEmpty()) activeSegment().flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Delete segments older than {@code maxAge} or beyond {@code maxTotalBytes}.
     * The active segment is never deleted.
     */
    public void applyRetention(Duration maxAge, long maxTotalBytes) throws IOException {
        lock.writeLock().lock();
        try {
            if (segments.size() <= 1) return; // never delete the only segment

            // Age-based retention
            if (maxAge != null) {
                long cutoffMs = System.currentTimeMillis() - maxAge.toMillis();
                segments.removeIf(seg -> {
                    if (seg == activeSegment()) return false;
                    try {
                        long lastModified = Files.getLastModifiedTime(
                                logDir.resolve(segmentName(seg.baseOffset()) + ".log")).toMillis();
                        if (lastModified < cutoffMs) {
                            deleteSegment(seg);
                            return true;
                        }
                    } catch (IOException e) {
                        log.warn("Failed to check segment age for {}", seg.baseOffset(), e);
                    }
                    return false;
                });
            }

            // Size-based retention
            if (maxTotalBytes > 0) {
                while (segments.size() > 1 && totalSizeBytes() > maxTotalBytes) {
                    LogSegment oldest = segments.getFirst();
                    deleteSegment(oldest);
                    segments.removeFirst();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            for (LogSegment seg : segments) {
                try { seg.close(); } catch (IOException e) {
                    log.warn("Error closing segment {}", seg.baseOffset(), e);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private LogSegment activeSegment() {
        return segments.getLast();
    }

    private LogSegment rollSegment(long newBaseOffset) throws IOException {
        log.debug("Rolling to new segment at offset {}", newBaseOffset);
        LogSegment newSeg = openSegment(newBaseOffset);
        segments.add(newSeg);
        return newSeg;
    }

    private LogSegment openSegment(long baseOffset) throws IOException {
        String name = segmentName(baseOffset);
        Path logPath   = logDir.resolve(name + ".log");
        Path indexPath = logDir.resolve(name + ".index");
        return new LogSegment(logPath, indexPath, baseOffset);
    }

    private void loadOrCreateSegments() throws IOException {
        // Find all .log files, sort by base offset
        try (Stream<Path> files = Files.list(logDir)) {
            List<Long> baseOffsets = files
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .map(p -> {
                        String name = p.getFileName().toString().replace(".log", "");
                        try { return Long.parseLong(name); }
                        catch (NumberFormatException e) { return -1L; }
                    })
                    .filter(offset -> offset >= 0)
                    .sorted()
                    .toList();

            for (long baseOffset : baseOffsets) {
                segments.add(openSegment(baseOffset));
            }
        }

        if (segments.isEmpty()) {
            segments.add(openSegment(0L));
        }
    }

    private int findSegmentIndexForOffset(long offset) {
        // Binary search: find the last segment whose baseOffset <= offset
        int lo = 0, hi = segments.size() - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            LogSegment seg = segments.get(mid);
            if (seg.baseOffset() <= offset) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    private long totalSizeBytes() {
        return segments.stream().mapToLong(LogSegment::sizeBytes).sum();
    }

    private void deleteSegment(LogSegment seg) {
        try {
            seg.close();
            Files.deleteIfExists(logDir.resolve(segmentName(seg.baseOffset()) + ".log"));
            Files.deleteIfExists(logDir.resolve(segmentName(seg.baseOffset()) + ".index"));
        } catch (IOException e) {
            log.warn("Failed to delete segment files for base offset {}", seg.baseOffset(), e);
        }
    }

    static String segmentName(long baseOffset) {
        return String.format("%020d", baseOffset);
    }
}

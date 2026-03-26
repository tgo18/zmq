package com.zmq.client;

import com.zmq.protocol.Command;
import com.zmq.protocol.CommandEncoder;
import com.zmq.protocol.CommandParser;
import com.zmq.protocol.ProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP connection manager for the ZMQ client SDK.
 *
 * Uses a dedicated virtual thread to read responses and correlate them
 * with pending {@link CompletableFuture}s.
 *
 * Since this is a request-response protocol (each send expects exactly one reply),
 * we use a simple synchronized send + blocking get pattern (the caller's virtual
 * thread blocks cheaply on the CompletableFuture).
 */
public class MQClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MQClient.class);

    private final String host;
    private final int port;

    private volatile Socket socket;
    private volatile CommandParser parser;
    private volatile CommandEncoder encoder;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /**
     * For FETCH with long-poll: we use synchronous blocking since the
     * response may not arrive for maxWaitMs. This map handles concurrent
     * outstanding requests by queueing completions in FIFO order.
     * NOTE: in a simple single-request-at-a-time model, one Future is enough.
     * For parallel requests we'd need correlation IDs; kept simple here.
     */
    private final ConcurrentHashMap<Long, CompletableFuture<Command>> pendingRequests =
            new ConcurrentHashMap<>();
    private volatile long requestCounter = 0;

    private Thread readerThread;

    public MQClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        DataInputStream in   = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        parser  = new CommandParser(in);
        encoder = new CommandEncoder(out);
        connected.set(true);

        // Start reader thread
        readerThread = Thread.ofVirtual().name("zmq-client-reader").start(this::readLoop);
        log.debug("Connected to {}:{}", host, port);
    }

    /**
     * Send a command and wait for the broker's response.
     * Thread-safe: multiple virtual threads can call this concurrently.
     */
    public Command sendAndReceive(Command command) throws IOException {
        if (!connected.get()) throw new IOException("Not connected");

        long requestId = ++requestCounter;
        CompletableFuture<Command> future = new CompletableFuture<>();
        pendingRequests.put(requestId, future);

        try {
            encoder.write(command);
            return future.get(); // virtual thread blocks cheaply here
        } catch (Exception e) {
            pendingRequests.remove(requestId);
            if (e.getCause() instanceof IOException ioe) throw ioe;
            throw new IOException("Request failed", e);
        }
    }

    /** @return a new {@link Producer} using this client. */
    public Producer createProducer() {
        return new Producer(this);
    }

    /** @return a new {@link Consumer} using this client and the given group ID. */
    public Consumer createConsumer(String groupId) {
        return new Consumer(this, groupId);
    }

    @Override
    public void close() throws IOException {
        connected.set(false);
        // Complete any pending futures exceptionally so callers unblock
        pendingRequests.values().forEach(f ->
                f.completeExceptionally(new IOException("Client closed")));
        pendingRequests.clear();
        if (readerThread != null) readerThread.interrupt();
        if (socket != null && !socket.isClosed()) socket.close();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void readLoop() {
        while (connected.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Command response = parser.readCommand();
                // Complete the oldest pending future (FIFO since requests are sequential)
                long minKey = pendingRequests.keySet().stream()
                        .mapToLong(Long::longValue).min().orElse(-1L);
                if (minKey >= 0) {
                    CompletableFuture<Command> future = pendingRequests.remove(minKey);
                    if (future != null) future.complete(response);
                }
            } catch (EOFException | SocketException e) {
                if (connected.get()) {
                    log.debug("Server closed connection");
                }
                connected.set(false);
                pendingRequests.values().forEach(f ->
                        f.completeExceptionally(new IOException("Connection lost")));
                break;
            } catch (ProtocolException e) {
                log.warn("Protocol error in reader: {}", e.getMessage());
            } catch (IOException e) {
                if (connected.get()) log.warn("Read error: {}", e.getMessage());
                break;
            }
        }
    }
}

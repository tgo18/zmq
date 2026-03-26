package com.zmq.protocol;

/**
 * Wire frame definition.
 *
 * Format: [MAGIC(2)] [TYPE(1)] [LENGTH(4)] [PAYLOAD(n)]
 *
 * MAGIC = 0x5A4D ("ZM") — identifies ZMQ protocol frames.
 * TYPE  = command type byte (see {@link Type}).
 * LENGTH = 4-byte big-endian signed int, payload byte count.
 * PAYLOAD = UTF-8 JSON body.
 */
public record Frame(byte type, byte[] payload) {

    public static final short MAGIC = 0x5A4D;
    public static final int HEADER_SIZE = 7; // 2 + 1 + 4
    public static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024; // 64 MB

    /** Command type byte constants — must match {@link Command} subtypes. */
    public static final class Type {
        private Type() {}

        public static final byte PUBLISH        = 0x01;
        public static final byte SUBSCRIBE      = 0x02;
        public static final byte UNSUBSCRIBE    = 0x03;
        public static final byte FETCH          = 0x04;
        public static final byte ACK            = 0x05;
        public static final byte CREATE_TOPIC   = 0x06;
        public static final byte DELETE_TOPIC   = 0x07;
        public static final byte LIST_TOPICS    = 0x08;
        public static final byte TOPIC_INFO     = 0x09;
        public static final byte PING           = 0x0A;
        public static final byte PONG           = 0x0B;
        public static final byte RESPONSE_OK    = 0x0C;
        public static final byte RESPONSE_ERROR = 0x0D;
        public static final byte MESSAGE_BATCH  = 0x0E;
    }
}

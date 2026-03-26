package com.zmq.message;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Instant;

/** Deserializes {@link Instant} from epoch milliseconds (a long). */
public class InstantMillisDeserializer extends StdDeserializer<Instant> {

    public InstantMillisDeserializer() {
        super(Instant.class);
    }

    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        return Instant.ofEpochMilli(p.getLongValue());
    }
}

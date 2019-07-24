package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.Iterator;

public class LongArrayCodec {
    public static void encodeNullable(ClientMessage clientMessage, long[] itemSeqs) {

    }

    public static long[] decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return new long[0];
    }
}

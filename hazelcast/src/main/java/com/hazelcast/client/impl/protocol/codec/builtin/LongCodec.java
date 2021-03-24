package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;

public class LongCodec {
    
    private LongCodec() {
    }
    
    public static void encode(ClientMessage message, Long value) {
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[LONG_SIZE_IN_BYTES]);
        FixedSizeTypesCodec.encodeLong(frame.content, 0, value);
        message.add(frame);
    }

    public static Long decode(ClientMessage.ForwardFrameIterator iterator) {
        return decode(iterator.next());
    }
    
    public static Long decode(ClientMessage.Frame frame) {
        return FixedSizeTypesCodec.decodeLong(frame.content, 0);
    }
}

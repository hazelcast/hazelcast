package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.HotRestartConfig;

import java.util.Iterator;

public class HotRestartConfigCodec {
    public static void encodeNullable(ClientMessage clientMessage, HotRestartConfig hotRestartConfig) {

    }

    public static HotRestartConfig decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

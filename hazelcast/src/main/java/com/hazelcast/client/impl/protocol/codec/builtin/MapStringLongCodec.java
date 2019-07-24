package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapStringLongCodec {
    public static void encode(ClientMessage clientMessage, Set<Map.Entry<String, Long>> set) {

    }

    public static List<Map.Entry<String, Long>> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

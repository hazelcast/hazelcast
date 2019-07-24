package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapStringStringCodec {
    public static void encode(ClientMessage clientMessage, Set<Map.Entry<String, String>> set) {

    }

    public static List<Map.Entry<String, String>> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

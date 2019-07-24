package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapStringMapIntegerLongCodec {
    public static void encode(ClientMessage clientMessage, Set<Map.Entry<String, List<Map.Entry<Integer, Long>>>> set) {

    }

    public static List<Map.Entry<String, List<Map.Entry<Integer, Long>>>> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

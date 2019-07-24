package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.serialization.Data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapDataDataCodec {
    public static void encode(ClientMessage clientMessage, Set<Map.Entry<Data, Data>> set) {

    }

    public static List<Map.Entry<Data, Data>> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

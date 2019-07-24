package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;

import java.util.Iterator;

public class QueryCacheEventDataCodec {
    public static void encode(ClientMessage clientMessage, QueryCacheEventData data) {

    }

    public static QueryCacheEventData decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

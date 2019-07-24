package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListQueryCacheEventDataCodec {
    public static void encode(ClientMessage clientMessage, Collection<QueryCacheEventData> queryCacheEventData) {

    }

    public static List<QueryCacheEventData> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

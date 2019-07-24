package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListCacheEventDataCodec {
    public static void encode(ClientMessage clientMessage, Collection<CacheEventData> cacheEventData) {

    }

    public static List<CacheEventData> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

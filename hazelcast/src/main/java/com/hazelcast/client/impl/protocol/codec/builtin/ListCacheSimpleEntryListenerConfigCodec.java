package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListCacheSimpleEntryListenerConfigCodec {
    public static void encodeNullable(ClientMessage clientMessage, Collection<CacheSimpleEntryListenerConfig> cacheSimpleEntryListenerConfigs) {

    }

    public static List<CacheSimpleEntryListenerConfig> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

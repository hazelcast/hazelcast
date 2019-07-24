package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListQueryCacheConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, Collection<QueryCacheConfigHolder> queryCacheConfigs) {

    }

    public static List<QueryCacheConfigHolder> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder;

import java.util.Iterator;
import java.util.List;

public class NearCacheConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, NearCacheConfigHolder configHolder) {

    }

    public static NearCacheConfigHolder decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

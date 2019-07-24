package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder;

import java.util.Iterator;
import java.util.List;

public class MapStoreConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, MapStoreConfigHolder configHolder) {

    }

    public static MapStoreConfigHolder decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

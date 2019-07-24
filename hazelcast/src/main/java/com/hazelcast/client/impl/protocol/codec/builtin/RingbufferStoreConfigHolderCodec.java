package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;

import java.util.Iterator;

public class RingbufferStoreConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, RingbufferStoreConfigHolder configHolder) {

    }

    public static RingbufferStoreConfigHolder decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

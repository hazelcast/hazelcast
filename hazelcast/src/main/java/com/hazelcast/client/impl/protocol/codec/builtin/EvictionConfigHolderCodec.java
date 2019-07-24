package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;

import java.util.Iterator;

public class EvictionConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, EvictionConfigHolder configHolder) {

    }

    public static EvictionConfigHolder decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

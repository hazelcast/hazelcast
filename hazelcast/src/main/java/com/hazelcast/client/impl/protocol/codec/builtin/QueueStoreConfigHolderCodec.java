package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueueStoreConfigHolder;

import java.util.Iterator;
import java.util.List;

public class QueueStoreConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, QueueStoreConfigHolder configHolder) {

    }

    public static QueueStoreConfigHolder decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

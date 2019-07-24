package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListListenerConfigHolderCodec {
    public static void encodeNullable(ClientMessage clientMessage, Collection<ListenerConfigHolder> listenerConfigHolders) {

    }

    public static List<ListenerConfigHolder> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

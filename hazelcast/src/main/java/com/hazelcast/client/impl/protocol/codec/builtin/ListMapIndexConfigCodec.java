package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.MapIndexConfig;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListMapIndexConfigCodec {
    public static void encodeNullable(ClientMessage clientMessage, Collection<MapIndexConfig> mapIndexConfigs) {

    }

    public static List<MapIndexConfig> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

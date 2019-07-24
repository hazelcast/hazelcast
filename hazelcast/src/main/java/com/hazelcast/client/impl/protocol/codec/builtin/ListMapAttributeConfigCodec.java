package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.MapAttributeConfig;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListMapAttributeConfigCodec {
    public static void encodeNullable(ClientMessage clientMessage, Collection<MapAttributeConfig> mapAttributeConfigs) {

    }

    public static List<MapAttributeConfig> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

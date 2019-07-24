package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.nio.serialization.Data;

import java.util.Iterator;

public class SimpleEntryViewCodec {
    public static void encodeNullable(ClientMessage clientMessage, SimpleEntryView<Data, Data> entryView) {

    }

    public static SimpleEntryView<Data, Data> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

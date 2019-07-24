package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.WanReplicationRef;

import java.util.Iterator;

public class WanReplicationRefCodec {
    public static void encodeNullable(ClientMessage clientMessage, WanReplicationRef wanReplicationRef) {

    }

    public static WanReplicationRef decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

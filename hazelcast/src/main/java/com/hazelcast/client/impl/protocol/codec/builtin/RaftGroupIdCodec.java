package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cp.internal.RaftGroupId;

import java.util.Iterator;

public class RaftGroupIdCodec {
    public static void encode(ClientMessage clientMessage, RaftGroupId groupId) {

    }

    public static RaftGroupId decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

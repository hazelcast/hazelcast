package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;

import java.util.Iterator;

public class MemberCodec {
    public static Member decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }

    public static void encode(ClientMessage clientMessage, Member member) {

    }
}

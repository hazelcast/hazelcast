package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListMemberCodec {
    public static void encode(ClientMessage clientMessage, Collection<Member> members) {

    }

    public static List<Member> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }

    public static void encodeNullable(ClientMessage clientMessage, Collection<Member> members) {

    }

    public static List<Member> decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

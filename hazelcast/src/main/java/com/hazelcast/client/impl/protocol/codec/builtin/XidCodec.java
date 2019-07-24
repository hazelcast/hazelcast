package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import javax.transaction.xa.Xid;
import java.util.Iterator;

public class XidCodec {
    public static void encode(ClientMessage clientMessage, Xid xid) {

    }

    public static Xid decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

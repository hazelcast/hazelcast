package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListDistributedObjectInfoCodec {
    public static void encode(ClientMessage clientMessage, Collection<DistributedObjectInfo> distributedObjectInfos) {

    }

    public static List<DistributedObjectInfo> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Address;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapAddressListIntegerCodec {
    public static void encode(ClientMessage clientMessage, Set<Map.Entry<Address, List<Integer>>> set) {

    }

    public static List<Map.Entry<Address, List<Integer>>> decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

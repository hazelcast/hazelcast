package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Address;

import java.util.Iterator;
import java.util.List;

public class AddressCodec {
    public static void encode(ClientMessage clientMessage, Address address) {

    }

    public static Address decode(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }

    public static void encodeNullable(ClientMessage clientMessage, Address address) {

    }

    public static Address decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

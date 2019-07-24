package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.CacheSimpleConfig;

import java.util.Iterator;

public class TimedExpiryPolicyFactoryConfigCodec {
    public static void encodeNullable(ClientMessage clientMessage, CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig config) {

    }

    public static CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig decodeNullable(Iterator<ClientMessage.Frame> iterator) {
        return null;
    }
}

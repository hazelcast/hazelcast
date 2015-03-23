package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.util.ParameterFlyweight;

public class MapPutParameters {

    public byte[] key;
    public byte[] value;
    public String name;
    public long threadId;
    public long ttl;
    public boolean async;

    public MapPutParameters(ParameterFlyweight flyweight) {
        name = flyweight.getStringUtf8();
        key = flyweight.getByteArray();
        value = flyweight.getByteArray();
        ttl = flyweight.getLong();
        threadId = flyweight.getLong();
        async = flyweight.getBoolean();
    }

    public static MapPutParameters decode(ParameterFlyweight flyweight) {
        return new MapPutParameters(flyweight);
    }

    public static void encode(ParameterFlyweight flyweight, String name, byte[] key, byte[] value, long threadId, long ttl, boolean async) {
        flyweight.set(name)
                .set(key)
                .set(value)
                .set(ttl)
                .set(threadId)
                .set(async);
    }

}

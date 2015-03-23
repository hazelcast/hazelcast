package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterFlyweight;

/**
 * Sample Put parameter
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD" })
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

    public static void encode(ParameterFlyweight flyweight, String name, byte[] key, byte[] value, long threadId, long ttl,
                              boolean async) {
        flyweight.set(name).set(key).set(value).set(ttl).set(threadId).set(async);
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int encodeSizeCost(String name, byte[] key, byte[] value) {

        return ClientMessage.HEADER_SIZE//
                + (BitUtil.SIZE_OF_INT + name.length() * 3)//
                + (BitUtil.SIZE_OF_INT + key.length)//
                + (BitUtil.SIZE_OF_INT + value.length)//
                + BitUtil.SIZE_OF_LONG//
                + BitUtil.SIZE_OF_LONG//
                + BitUtil.SIZE_OF_BYTE;
    }

}

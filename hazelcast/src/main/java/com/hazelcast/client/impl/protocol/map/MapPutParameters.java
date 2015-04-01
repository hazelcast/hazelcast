package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;

/**
 * Sample Put parameter
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class MapPutParameters extends ClientMessage {

    /**
     * ClientMessageType of this message
     */
    public static final MapMessageType TYPE = MapMessageType.MAP_PUT;
    public byte[] key;
    public byte[] value;
    public String name;
    public long threadId;
    public long ttl;
    public boolean async;

    private MapPutParameters() {
    }

    private MapPutParameters(ClientMessage flyweight) {
        name = flyweight.getStringUtf8();
        key = flyweight.getByteArray();
        value = flyweight.getByteArray();
        ttl = flyweight.getLong();
        threadId = flyweight.getLong();
        async = flyweight.getBoolean();
    }

    public static MapPutParameters decode(ClientMessage flyweight) {
        return new MapPutParameters(flyweight);
    }

    public static MapPutParameters encode(String name, byte[] key, byte[] value, long threadId, long ttl, boolean async) {
        MapPutParameters parameters = new MapPutParameters();
        final int requiredDataSize = calculateDataSize(name, key, value, threadId, ttl, async);
        parameters.ensureCapacity(requiredDataSize);
        parameters.setMessageType(TYPE.id());
        parameters.set(name).set(key).set(value).set(ttl).set(threadId).set(async);
        return parameters;
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int calculateDataSize(String name, byte[] key, byte[] value, long threadId, long ttl, boolean async) {
        return ClientMessage.HEADER_SIZE//
                + (BitUtil.SIZE_OF_INT + name.length() * 3)//
                + (BitUtil.SIZE_OF_INT + key.length)//
                + (BitUtil.SIZE_OF_INT + value.length)//
                + BitUtil.SIZE_OF_LONG//
                + BitUtil.SIZE_OF_LONG//
                + BitUtil.SIZE_OF_BYTE;
    }


}

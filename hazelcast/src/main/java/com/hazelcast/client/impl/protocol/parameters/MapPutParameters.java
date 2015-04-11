package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

/**
 * Sample Put parameter
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class MapPutParameters /*extends ClientMessage*/ {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.MAP_PUT_REQUEST;
    public byte[] key;
    public byte[] value;
    public String name;
    public long threadId;
    public long ttl;
    public boolean async;


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

    public static ClientMessage encode(String name, byte[] key, byte[] value, long threadId, long ttl, boolean async) {
        final int requiredDataSize = calculateDataSize(name, key, value, threadId, ttl, async);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(name).set(key).set(value).set(ttl).set(threadId).set(async);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int calculateDataSize(String name, byte[] key, byte[] value, long threadId, long ttl, boolean async) {
        return ClientMessage.HEADER_SIZE//
                + ParameterUtil.calculateStringDataSize(name)
                + ParameterUtil.calculateByteArrayDataSize(key)
                + ParameterUtil.calculateByteArrayDataSize(value)
                + BitUtil.SIZE_OF_LONG//
                + BitUtil.SIZE_OF_LONG//
                + BitUtil.SIZE_OF_BYTE;
    }

}

package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.nio.ByteBuffer;

public class MapPutMessage extends ClientMessage{

    private byte[] key;
    private byte[] value;
    private String name;

    private long threadId;
    private long ttl;


    public void wrapForEncode(ByteBuffer buffer, int offset, String name, byte[] key, byte[] value, long threadId, long ttl) {
        super.wrapForEncode(buffer, offset);
        putVarData(name.getBytes());
        putVarData(key);
        putVarData(value);
    }


}

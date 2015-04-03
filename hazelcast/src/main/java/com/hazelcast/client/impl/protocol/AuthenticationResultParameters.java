package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.core.Client;

/**
 * AuthenticationResultParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationResultParameters {

    public String host;
    public int port;
    public String uuid;
    public String ownerUuid;

    private AuthenticationResultParameters(ClientMessage flyweight) {
        host = flyweight.getStringUtf8();
        port = flyweight.getInt();
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
    }

    public static AuthenticationResultParameters decode(ClientMessage flyweight) {
        return new AuthenticationResultParameters(flyweight);
    }

    public static ClientMessage encode(String host, int port, String uuid, String ownerUuid) {
        final int requiredDataSize = calculateDataSize(host, port, uuid, ownerUuid);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.set(host).set(port).set(uuid).set(ownerUuid);
        clientMessage.updateFrameLenght();
        return clientMessage;
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int calculateDataSize(String host, int port, String uuid, String ownerUuid) {
        return ClientMessage.HEADER_SIZE//
                + (BitUtil.SIZE_OF_INT + host.length() * 3)//host
                + BitUtil.SIZE_OF_INT//port
                + (BitUtil.SIZE_OF_INT + uuid.length() * 3)//
                + (BitUtil.SIZE_OF_INT + ownerUuid.length() * 3);//
    }

}

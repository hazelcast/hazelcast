package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.util.BitUtil;

/**
 * AuthenticationResultParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationResultParameters
        extends ClientMessage {

    public String host;
    public int port;
    public String uuid;
    public String ownerUuid;

    private AuthenticationResultParameters() {
    }

    private AuthenticationResultParameters(ClientMessage flyweight) {
        host = flyweight.getStringUtf8();
        port = flyweight.getInt();
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
    }

    public static AuthenticationResultParameters decode(ClientMessage flyweight) {
        return new AuthenticationResultParameters(flyweight);
    }

    public static AuthenticationResultParameters encode(String host, int port, String uuid, String ownerUuid) {
        AuthenticationResultParameters parameters = new AuthenticationResultParameters();
        final int requiredDataSize = calculateDataSize(host, port, uuid, ownerUuid);
        parameters.ensureCapacity(requiredDataSize);
        parameters.set(host).set(port).set(uuid).set(ownerUuid);
        return parameters;
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

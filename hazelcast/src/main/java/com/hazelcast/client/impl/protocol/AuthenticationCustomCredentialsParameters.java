package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.util.BitUtil;

/**
 * AuthenticationParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationCustomCredentialsParameters
        extends ClientMessage {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.AUTHENTICATION_CUSTOM;
    public byte[] credentials;
    public String uuid;
    public String ownerUuid;
    public boolean isOwnerConnection;

    private AuthenticationCustomCredentialsParameters() {
    }

    private AuthenticationCustomCredentialsParameters(ClientMessage flyweight) {
        credentials = flyweight.getByteArray();
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
        isOwnerConnection = flyweight.getBoolean();
    }

    /**
     * Decode input byte array data into parameters
     * @param flyweight
     * @return AuthenticationCustomParameters
     */
    public static AuthenticationCustomCredentialsParameters decode(ClientMessage flyweight) {
        return new AuthenticationCustomCredentialsParameters(flyweight);
    }

    /**
     * Encode parameters into byte array, i.e. ClientMessage
     * @param credentials
     * @param uuid
     * @param ownerUuid
     * @param isOwnerConnection
     * @return AuthenticationCustomParameters
     */
    public static AuthenticationCustomCredentialsParameters encode(byte[] credentials, String uuid, String ownerUuid,
                                                        boolean isOwnerConnection) {
        AuthenticationCustomCredentialsParameters parameters = new AuthenticationCustomCredentialsParameters();
        final int requiredDataSize = calculateDataSize(credentials, uuid, ownerUuid, isOwnerConnection);
        parameters.ensureCapacity(requiredDataSize);
        parameters.setMessageType(TYPE.id());
        parameters.set(credentials).set(uuid).set(ownerUuid).set(isOwnerConnection);
        return parameters;
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int calculateDataSize(byte[] credentials, String uuid, String ownerUuid, boolean isOwnerConnection) {
        return ClientMessage.HEADER_SIZE//
                + (BitUtil.SIZE_OF_INT + credentials.length)//
                + (BitUtil.SIZE_OF_INT + uuid.length() * 3)//
                + (BitUtil.SIZE_OF_INT + ownerUuid.length() * 3)//
                + BitUtil.SIZE_OF_BYTE;
    }

}

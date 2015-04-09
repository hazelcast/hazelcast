package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

/**
 * AuthenticationParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationParameters {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.AUTHENTICATION_DEFAULT_REQUEST;
    public String username;
    public String password;
    public String uuid;
    public String ownerUuid;
    public boolean isOwnerConnection;


    private AuthenticationParameters(ClientMessage flyweight) {
        username = flyweight.getStringUtf8();
        password = flyweight.getStringUtf8();
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
        isOwnerConnection = flyweight.getBoolean();
    }

    /**
     * Decode input byte array data into parameters
     * @param flyweight
     * @return AuthenticationParameters
     */
    public static AuthenticationParameters decode(ClientMessage flyweight) {
        return new AuthenticationParameters(flyweight);
    }

    /**
     * Encode parameters into byte array, i.e. ClientMessage
     * @param username
     * @param password
     * @param uuid
     * @param ownerUuid
     * @param isOwnerConnection
     * @return encoded ClientMessage
     */
    public static ClientMessage encode(String username, String password, String uuid, String ownerUuid,
                                       boolean isOwnerConnection) {
        final int requiredDataSize = calculateDataSize(username, password, uuid, ownerUuid, isOwnerConnection);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(username).set(password).set(uuid).set(ownerUuid).set(isOwnerConnection);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int calculateDataSize(String username, String password, String uuid, String ownerUuid,
                                        boolean isOwnerConnection) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateStringDataSize(username)
                + ParameterUtil.calculateStringDataSize(password)
                + ParameterUtil.calculateStringDataSize(uuid)
                + ParameterUtil.calculateStringDataSize(ownerUuid)
                + BitUtil.SIZE_OF_BYTE;
    }

}

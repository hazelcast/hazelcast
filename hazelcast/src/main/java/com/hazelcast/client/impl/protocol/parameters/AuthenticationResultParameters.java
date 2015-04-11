package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;

/**
 * AuthenticationResultParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationResultParameters {

    public static final ClientMessageType TYPE = ClientMessageType.AUTHENTICATION_RESULT;
    public Address address;
    public String uuid;
    public String ownerUuid;

    private AuthenticationResultParameters(ClientMessage flyweight) throws UnknownHostException {
        address = ParameterUtil.decodeAddress(flyweight);
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
    }

    public static AuthenticationResultParameters decode(ClientMessage flyweight) throws UnknownHostException {
        return new AuthenticationResultParameters(flyweight);
    }

    public static ClientMessage encode(Address address, String uuid, String ownerUuid) {
        final int requiredDataSize = calculateDataSize(address, uuid, ownerUuid);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        ParameterUtil.encodeAddress(clientMessage, address);
        clientMessage.set(uuid).set(ownerUuid);

        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Address address, String uuid, String ownerUuid) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateAddressDataSize(address)
                + ParameterUtil.calculateStringDataSize(uuid)
                + ParameterUtil.calculateStringDataSize(ownerUuid);
    }

}

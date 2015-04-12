package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class CreateProxyParameters {

    public static final ClientMessageType TYPE = ClientMessageType.CREATE_PROXY_REQUEST;
    public String name;
    public String serviceName;

    private CreateProxyParameters(ClientMessage flyweight) {
        name = flyweight.getStringUtf8();
        serviceName = flyweight.getStringUtf8();
    }

    public static CreateProxyParameters decode(ClientMessage flyweight) {
        return new CreateProxyParameters(flyweight);
    }

    public static ClientMessage encode(String name, String serviceName) {
        final int requiredDataSize = calculateDataSize(name, serviceName);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(name).set(serviceName);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(String name, String serviceName) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateStringDataSize(name)
                + ParameterUtil.calculateStringDataSize(serviceName);
    }
}

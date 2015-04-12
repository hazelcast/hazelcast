package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class GetPartitionsParameters {

    public static final ClientMessageType TYPE = ClientMessageType.GET_PARTITIONS_REQUEST;

    private GetPartitionsParameters(ClientMessage flyweight) {
    }

    public static GetPartitionsParameters decode(ClientMessage flyweight) {
        return new GetPartitionsParameters(flyweight);
    }

    public static ClientMessage encode() {
        final int requiredDataSize = calculateDataSize();
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize() {
        return ClientMessage.HEADER_SIZE;
    }
}

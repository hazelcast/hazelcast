package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class TransactionRecoverAllParameters {


    public static final ClientMessageType TYPE = ClientMessageType.TRANSACTION_RECOVER_ALL;

    private TransactionRecoverAllParameters(ClientMessage clientMessage) {
    }

    public static TransactionRecoverAllParameters decode(ClientMessage clientMessage) {
        return new TransactionRecoverAllParameters(clientMessage);
    }

    public static ClientMessage encode() {
        final int requiredDataSize = calculateDataSize();
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize() {
        return ClientMessage.HEADER_SIZE;
    }


}



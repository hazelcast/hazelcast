package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.map.MapMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;

/**
 * Sample Put parameter
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class GenericResultParameters
        extends ClientMessage {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.RESULT;
    public byte[] result;

    private GenericResultParameters() {
    }

    private GenericResultParameters(ClientMessage flyweight) {
        result = flyweight.getByteArray();
    }

    public static GenericResultParameters decode(ClientMessage flyweight) {
        return new GenericResultParameters(flyweight);
    }

    public static GenericResultParameters encode(byte[] result) {
        GenericResultParameters parameters = new GenericResultParameters();
        final int requiredDataSize = calculateDataSize(result);
        parameters.ensureCapacity(requiredDataSize);
        parameters.setMessageType(TYPE.id());
        parameters.set(result);
        return parameters;
    }

    /**
     * sample data size estimation
     * @return size
     */
    public static int calculateDataSize(byte[] result) {
        return ClientMessage.HEADER_SIZE//
                + (BitUtil.SIZE_OF_INT + result.length);
    }


}

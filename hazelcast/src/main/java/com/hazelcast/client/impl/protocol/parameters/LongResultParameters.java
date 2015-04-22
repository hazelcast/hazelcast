package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;

/**
 * Created by muratayan on 4/22/15.
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class LongResultParameters {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.LONG_RESULT;
    public long result;

    private LongResultParameters(ClientMessage flyweight) {
        result = flyweight.getLong();
    }

    public static LongResultParameters decode(ClientMessage flyweight) {
        return new LongResultParameters(flyweight);
    }

    public static ClientMessage encode(long result) {
        final int requiredDataSize = calculateDataSize(result);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(result);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(long result) {
        return ClientMessage.HEADER_SIZE
                + BitUtil.SIZE_OF_LONG;
    }
}



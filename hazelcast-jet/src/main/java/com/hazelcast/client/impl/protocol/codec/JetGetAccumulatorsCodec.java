package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import javax.annotation.Generated;

@Generated("Hazelcast.code.generator")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class JetGetAccumulatorsCodec {

    public static final JetMessageType REQUEST_TYPE = JetMessageType.JET_GETACCUMULATORS;
    public static final int RESPONSE_TYPE = 105;
    public static final boolean RETRYABLE = false;

    //************************ REQUEST *************************//

    public static class RequestParameters {
    public static final JetMessageType TYPE = REQUEST_TYPE;
        public java.lang.String name;

        public static int calculateDataSize(java.lang.String
 name) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += ParameterUtil.calculateDataSize(name);
            return dataSize;
        }
    }

    public static ClientMessage encodeRequest(java.lang.String
 name) {
        final int requiredDataSize = RequestParameters.calculateDataSize(name);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(RETRYABLE);
        clientMessage.set(name);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        final RequestParameters parameters = new RequestParameters();
        java.lang.String name = null;
        name = clientMessage.getStringUtf8();
            parameters.name = name;
        return parameters;
    }

    //************************ RESPONSE *************************//

    public static class ResponseParameters {
        public com.hazelcast.nio.serialization.Data response;

        public static int calculateDataSize(com.hazelcast.nio.serialization.Data
 response) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
            if (response != null) {
            dataSize += ParameterUtil.calculateDataSize(response);
            }
            return dataSize;
        }
    }

    public static ClientMessage encodeResponse(com.hazelcast.nio.serialization.Data
 response) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(response);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
        boolean response_isNull;
        if (response == null) {
            response_isNull = true;
            clientMessage.set(response_isNull);
        } else {
            response_isNull= false;
            clientMessage.set(response_isNull);
        clientMessage.set(response);
        }
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ResponseParameters parameters = new ResponseParameters();
        com.hazelcast.nio.serialization.Data response = null;
        boolean response_isNull = clientMessage.getBoolean();
        if (!response_isNull) {
        response = clientMessage.getData();
            parameters.response = response;
        }
        return parameters;
    }

}

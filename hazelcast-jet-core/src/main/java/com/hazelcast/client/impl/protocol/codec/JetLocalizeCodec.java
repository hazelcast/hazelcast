/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import javax.annotation.Generated;

@Generated("Hazelcast.code.generator")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class JetLocalizeCodec {

    public static final JetMessageType REQUEST_TYPE = JetMessageType.JET_LOCALIZE;
    public static final int RESPONSE_TYPE = 101;
    public static final boolean RETRYABLE = false;

    //************************ REQUEST *************************//

    public static class RequestParameters {
    public static final JetMessageType TYPE = REQUEST_TYPE;
        public java.lang.String name;
        public com.hazelcast.nio.serialization.Data chunk;

        public static int calculateDataSize(java.lang.String
 name, com.hazelcast.nio.serialization.Data
 chunk) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += ParameterUtil.calculateDataSize(name);
            dataSize += ParameterUtil.calculateDataSize(chunk);
            return dataSize;
        }
    }

    public static ClientMessage encodeRequest(java.lang.String
 name, com.hazelcast.nio.serialization.Data
 chunk) {
        final int requiredDataSize = RequestParameters.calculateDataSize(name, chunk);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(RETRYABLE);
        clientMessage.set(name);
        clientMessage.set(chunk);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        final RequestParameters parameters = new RequestParameters();
        java.lang.String name = null;
        name = clientMessage.getStringUtf8();
            parameters.name = name;
        com.hazelcast.nio.serialization.Data chunk = null;
        chunk = clientMessage.getData();
            parameters.chunk = chunk;
        return parameters;
    }

    //************************ RESPONSE *************************//

    public static class ResponseParameters {
        public boolean response;

        public static int calculateDataSize(boolean
 response) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
            return dataSize;
        }
    }

    public static ClientMessage encodeResponse(boolean
 response) {
        final int requiredDataSize = ResponseParameters.calculateDataSize(response);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(RESPONSE_TYPE);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ResponseParameters parameters = new ResponseParameters();
        boolean response ;
        response = clientMessage.getBoolean();
            parameters.response = response;
        return parameters;
    }

}

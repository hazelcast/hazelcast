/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;

import javax.annotation.Nullable;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 */
@Generated("eefec3deee3e9fc8aff1563d4b5258da")
public final class JetGetJobIdsCodec {
    //hex: 0xFE0400
    public static final int REQUEST_MESSAGE_TYPE = 16647168;
    //hex: 0xFE0401
    public static final int RESPONSE_MESSAGE_TYPE = 16647169;
    private static final int REQUEST_ONLY_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_ONLY_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private JetGetJobIdsCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         */
        public @Nullable java.lang.String onlyName;

        /**
         */
        public long onlyJobId;

        /**
         * True if the onlyName is received from the client, false otherwise.
         * If this is false, onlyName has the default value for its type.
         */
        public boolean isOnlyNameExists;

        /**
         * True if the onlyJobId is received from the client, false otherwise.
         * If this is false, onlyJobId has the default value for its type.
         */
        public boolean isOnlyJobIdExists;
    }

    public static ClientMessage encodeRequest(@Nullable java.lang.String onlyName, long onlyJobId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("Jet.GetJobIds");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_ONLY_JOB_ID_FIELD_OFFSET, onlyJobId);
        clientMessage.add(initialFrame);
        CodecUtil.encodeNullable(clientMessage, onlyName, StringCodec::encode);
        return clientMessage;
    }

    public static JetGetJobIdsCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        if (initialFrame.content.length >= REQUEST_ONLY_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES) {
            request.onlyJobId = decodeLong(initialFrame.content, REQUEST_ONLY_JOB_ID_FIELD_OFFSET);
            request.isOnlyJobIdExists = true;
        } else {
            request.isOnlyJobIdExists = false;
        }
        if (iterator.hasNext()) {
            request.onlyName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
            request.isOnlyNameExists = true;
        } else {
            request.isOnlyNameExists = false;
        }
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         */
        public com.hazelcast.internal.serialization.Data response;

        /**
         * True if the response is received from the member, false otherwise.
         * If this is false, response has the default value for its type.
         */
        public boolean isResponseExists;
    }

    public static ClientMessage encodeResponse(com.hazelcast.internal.serialization.Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, response);
        return clientMessage;
    }

    public static JetGetJobIdsCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        if (iterator.hasNext()) {
            response.response = DataCodec.decode(iterator);
            response.isResponseExists = true;
        } else {
            response.isResponseExists = false;
        }
        return response;
    }
}

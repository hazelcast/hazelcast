/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * Applies a function on the value
 */
public class CPAtomicRefApplyCodec {

        private static final int REQUEST_RETURN_VALUE_TYPE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int REQUEST_ALTER_FIELD_OFFSET = REQUEST_RETURN_VALUE_TYPE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_ALTER_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 9217;//hex: 0x2401,
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 105;//hex: 0x0069,

    public static class RequestParameters {

        /**
         * CP group id of this IAtomicReference instance.
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * The name of this IAtomicReference instance.
         */
        public java.lang.String name;

        /**
         * The function applied to the value.
         */
        public com.hazelcast.nio.serialization.Data function;

        /**
         * 0 returns no value, 1 returns the old value,
         * 2 returns the new value
         */
        public int returnValueType;

        /**
         * Denotes whether result of the function will be
         * set to the IAtomicRefInstance
         */
        public boolean alter;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name, com.hazelcast.nio.serialization.Data function, int returnValueType, boolean alter) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("CPAtomicRef.Apply");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_RETURN_VALUE_TYPE_FIELD_OFFSET, returnValueType);
        encodeBoolean(initialFrame.content, REQUEST_ALTER_FIELD_OFFSET, alter);
        clientMessage.addFrame(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, function);
        return clientMessage;
    }

    public static CPAtomicRefApplyCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.returnValueType = decodeInt(initialFrame.content, REQUEST_RETURN_VALUE_TYPE_FIELD_OFFSET);
        request.alter = decodeBoolean(initialFrame.content, REQUEST_ALTER_FIELD_OFFSET);
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.name = StringCodec.decode(iterator);
        request.function = DataCodec.decode(iterator);
        return request;
    }

    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public com.hazelcast.nio.serialization.Data response;
    }

    public static ClientMessage encodeResponse(com.hazelcast.nio.serialization.Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        CodecUtil.encodeNullable(clientMessage, response, DataCodec::encode);
        return clientMessage;
    }

    public static CPAtomicRefApplyCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        iterator.next();//empty initial frame
        response.response = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        return response;
    }

}
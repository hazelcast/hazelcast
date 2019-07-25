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
 * Queries the map based on the specified predicate and returns the keys of matching entries. Specified predicate
 * runs on all members in parallel.The set is NOT backed by the map, so changes to the map are NOT reflected in the
 * set, and vice-versa. This method is always executed by a distributed query, so it may throw a
 * QueryResultSizeExceededException if query result size limit is configured.
 */
public class TransactionalMapKeySetWithPredicateCodec {

        private static final int REQUEST_THREAD_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_THREAD_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 4111;//hex: 0x100F,
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 106;//hex: 0x006A,

    public static class RequestParameters {

        /**
         * Name of the Transactional Map
         */
        public java.lang.String name;

        /**
         * ID of the this transaction operation
         */
        public java.lang.String txnId;

        /**
         * The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
         */
        public long threadId;

        /**
         * Specified query criteria.
         */
        public com.hazelcast.nio.serialization.Data predicate;
    }

    public static ClientMessage encodeRequest(java.lang.String name, java.lang.String txnId, long threadId, com.hazelcast.nio.serialization.Data predicate) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("TransactionalMap.KeySetWithPredicate");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET, threadId);
        clientMessage.addFrame(initialFrame);
        StringCodec.encode(clientMessage, name);
        StringCodec.encode(clientMessage, txnId);
        DataCodec.encode(clientMessage, predicate);
        return clientMessage;
    }

    public static TransactionalMapKeySetWithPredicateCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.threadId = decodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.txnId = StringCodec.decode(iterator);
        request.predicate = DataCodec.decode(iterator);
        return request;
    }

    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public java.util.List<com.hazelcast.nio.serialization.Data> response;
    }

    public static ClientMessage encodeResponse(java.util.Collection<com.hazelcast.nio.serialization.Data> response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, response, DataCodec::encode);
        return clientMessage;
    }

    public static TransactionalMapKeySetWithPredicateCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        iterator.next();//empty initial frame
        response.response = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
        return response;
    }

}
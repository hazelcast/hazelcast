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
 * Fetches the specified number of entries from the specified partition starting from specified table index
 * that match the predicate and applies the projection logic on them.
 */
public class MapFetchWithQueryCodec {

        private static final int REQUEST_TABLE_INDEX_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int REQUEST_BATCH_FIELD_OFFSET = REQUEST_TABLE_INDEX_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_BATCH_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 326;//hex: 0x0146,
        private static final int RESPONSE_NEXT_TABLE_INDEX_TO_READ_FROM_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 124;//hex: 0x007C,

    public static class RequestParameters {

        /**
         * Name of the map
         */
        public java.lang.String name;

        /**
         * The slot number (or index) to start the iterator
         */
        public int tableIndex;

        /**
         * The number of items to be batched
         */
        public int batch;

        /**
         * projection to transform the entries with
         */
        public com.hazelcast.nio.serialization.Data projection;

        /**
         * predicate to filter the entries with
         */
        public com.hazelcast.nio.serialization.Data predicate;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int tableIndex, int batch, com.hazelcast.nio.serialization.Data projection, com.hazelcast.nio.serialization.Data predicate) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Map.FetchWithQuery");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_TABLE_INDEX_FIELD_OFFSET, tableIndex);
        encodeInt(initialFrame.content, REQUEST_BATCH_FIELD_OFFSET, batch);
        clientMessage.addFrame(initialFrame);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, projection);
        DataCodec.encode(clientMessage, predicate);
        return clientMessage;
    }

    public static MapFetchWithQueryCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.tableIndex = decodeInt(initialFrame.content, REQUEST_TABLE_INDEX_FIELD_OFFSET);
        request.batch = decodeInt(initialFrame.content, REQUEST_BATCH_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.projection = DataCodec.decode(iterator);
        request.predicate = DataCodec.decode(iterator);
        return request;
    }

    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public java.util.List<com.hazelcast.nio.serialization.Data> results;

        /**
         * TODO DOC
         */
        public int nextTableIndexToReadFrom;
    }

    public static ClientMessage encodeResponse(java.util.Collection<com.hazelcast.nio.serialization.Data> results, int nextTableIndexToReadFrom) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        encodeInt(initialFrame.content, RESPONSE_NEXT_TABLE_INDEX_TO_READ_FROM_FIELD_OFFSET, nextTableIndexToReadFrom);
        ListMultiFrameCodec.encode(clientMessage, results, DataCodec::encode);
        return clientMessage;
    }

    public static MapFetchWithQueryCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.nextTableIndexToReadFrom = decodeInt(initialFrame.content, RESPONSE_NEXT_TABLE_INDEX_TO_READ_FROM_FIELD_OFFSET);
        response.results = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
        return response;
    }

}
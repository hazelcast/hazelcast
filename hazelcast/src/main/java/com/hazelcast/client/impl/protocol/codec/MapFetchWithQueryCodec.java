/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * Fetches the specified number of entries from the specified partition starting from specified table index
 * that match the predicate and applies the projection logic on them.
 */
@Generated("1b1f815903dbcaa5b1633720e7f95fb8")
public final class MapFetchWithQueryCodec {
    //hex: 0x014000
    public static final int REQUEST_MESSAGE_TYPE = 81920;
    //hex: 0x014001
    public static final int RESPONSE_MESSAGE_TYPE = 81921;
    private static final int REQUEST_BATCH_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_BATCH_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private MapFetchWithQueryCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the map
         */
        public java.lang.String name;

        /**
         * The index-size pairs that define the state of iteration
         */
        public java.util.List<java.util.Map.Entry<java.lang.Integer, java.lang.Integer>> iterationPointers;

        /**
         * The number of items to be batched
         */
        public int batch;

        /**
         * projection to transform the entries with
         */
        public com.hazelcast.internal.serialization.Data projection;

        /**
         * predicate to filter the entries with
         */
        public com.hazelcast.internal.serialization.Data predicate;
    }

    public static ClientMessage encodeRequest(java.lang.String name, java.util.Collection<java.util.Map.Entry<java.lang.Integer, java.lang.Integer>> iterationPointers, int batch, com.hazelcast.internal.serialization.Data projection, com.hazelcast.internal.serialization.Data predicate) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("Map.FetchWithQuery");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_BATCH_FIELD_OFFSET, batch);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        EntryListIntegerIntegerCodec.encode(clientMessage, iterationPointers);
        DataCodec.encode(clientMessage, projection);
        DataCodec.encode(clientMessage, predicate);
        return clientMessage;
    }

    public static MapFetchWithQueryCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.batch = decodeInt(initialFrame.content, REQUEST_BATCH_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.iterationPointers = EntryListIntegerIntegerCodec.decode(iterator);
        request.projection = DataCodec.decode(iterator);
        request.predicate = DataCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * List of fetched entries.
         */
        public java.util.List<com.hazelcast.internal.serialization.Data> results;

        /**
         * The index-size pairs that define the state of iteration
         */
        public java.util.List<java.util.Map.Entry<java.lang.Integer, java.lang.Integer>> iterationPointers;
    }

    public static ClientMessage encodeResponse(java.util.Collection<com.hazelcast.internal.serialization.Data> results, java.util.Collection<java.util.Map.Entry<java.lang.Integer, java.lang.Integer>> iterationPointers) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encodeContainsNullable(clientMessage, results, DataCodec::encode);
        EntryListIntegerIntegerCodec.encode(clientMessage, iterationPointers);
        return clientMessage;
    }

    public static MapFetchWithQueryCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.results = ListMultiFrameCodec.decodeContainsNullable(iterator, DataCodec::decode);
        response.iterationPointers = EntryListIntegerIntegerCodec.decode(iterator);
        return response;
    }

}

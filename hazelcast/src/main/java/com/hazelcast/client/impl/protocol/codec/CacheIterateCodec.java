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
 * The ordering of iteration over entries is undefined. During iteration, any entries that are a). read will have
 * their appropriate CacheEntryReadListeners notified and b). removed will have their appropriate
 * CacheEntryRemoveListeners notified. java.util.Iterator#next() may return null if the entry is no longer present,
 * has expired or has been evicted.
 */
@Generated("1d49a57e149a5fa63c068a8526dc8b29")
public final class CacheIterateCodec {
    //hex: 0x150F00
    public static final int REQUEST_MESSAGE_TYPE = 1380096;
    //hex: 0x150F01
    public static final int RESPONSE_MESSAGE_TYPE = 1380097;
    private static final int REQUEST_TABLE_INDEX_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_BATCH_FIELD_OFFSET = REQUEST_TABLE_INDEX_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_BATCH_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_TABLE_INDEX_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_TABLE_INDEX_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private CacheIterateCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the cache.
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
    }

    public static ClientMessage encodeRequest(java.lang.String name, int tableIndex, int batch) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Cache.Iterate");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_TABLE_INDEX_FIELD_OFFSET, tableIndex);
        encodeInt(initialFrame.content, REQUEST_BATCH_FIELD_OFFSET, batch);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CacheIterateCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.tableIndex = decodeInt(initialFrame.content, REQUEST_TABLE_INDEX_FIELD_OFFSET);
        request.batch = decodeInt(initialFrame.content, REQUEST_BATCH_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * The slot number (or index) to start the iterator
         */
        public int tableIndex;

        /**
         * TODO DOC
         */
        public java.util.List<com.hazelcast.nio.serialization.Data> keys;
    }

    public static ClientMessage encodeResponse(int tableIndex, java.util.Collection<com.hazelcast.nio.serialization.Data> keys) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeInt(initialFrame.content, RESPONSE_TABLE_INDEX_FIELD_OFFSET, tableIndex);
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, keys, DataCodec::encode);
        return clientMessage;
    }

    public static CacheIterateCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.tableIndex = decodeInt(initialFrame.content, RESPONSE_TABLE_INDEX_FIELD_OFFSET);
        response.keys = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
        return response;
    }

}

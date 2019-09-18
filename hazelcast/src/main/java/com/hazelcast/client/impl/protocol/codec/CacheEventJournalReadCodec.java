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
 * Reads from the cache event journal in batches. You may specify the start sequence,
 * the minimum required number of items in the response, the maximum number of items
 * in the response, a predicate that the events should pass and a projection to
 * apply to the events in the journal.
 * If the event journal currently contains less events than {@code minSize}, the
 * call will wait until it has sufficient items.
 * The predicate, filter and projection may be {@code null} in which case all elements are returned
 * and no projection is applied.
 */
public final class CacheEventJournalReadCodec {
    //hex: 0x152200
    public static final int REQUEST_MESSAGE_TYPE = 1384960;
    //hex: 0x152201
    public static final int RESPONSE_MESSAGE_TYPE = 1384961;
    private static final int REQUEST_START_SEQUENCE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MIN_SIZE_FIELD_OFFSET = REQUEST_START_SEQUENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_MAX_SIZE_FIELD_OFFSET = REQUEST_MIN_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_MAX_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_READ_COUNT_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_NEXT_SEQ_FIELD_OFFSET = RESPONSE_READ_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_NEXT_SEQ_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private CacheEventJournalReadCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of the cache
         */
        public java.lang.String name;

        /**
         * the startSequence of the first item to read
         */
        public long startSequence;

        /**
         * the minimum number of items to read.
         */
        public int minSize;

        /**
         * the maximum number of items to read.
         */
        public int maxSize;

        /**
         * the predicate to apply before processing events
         */
        public com.hazelcast.nio.serialization.Data predicate;

        /**
         * the projection to apply to journal events
         */
        public com.hazelcast.nio.serialization.Data projection;
    }

    public static ClientMessage encodeRequest(java.lang.String name, long startSequence, int minSize, int maxSize, com.hazelcast.nio.serialization.Data predicate, com.hazelcast.nio.serialization.Data projection) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Cache.EventJournalRead");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeLong(initialFrame.content, REQUEST_START_SEQUENCE_FIELD_OFFSET, startSequence);
        encodeInt(initialFrame.content, REQUEST_MIN_SIZE_FIELD_OFFSET, minSize);
        encodeInt(initialFrame.content, REQUEST_MAX_SIZE_FIELD_OFFSET, maxSize);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        CodecUtil.encodeNullable(clientMessage, predicate, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, projection, DataCodec::encode);
        return clientMessage;
    }

    public static CacheEventJournalReadCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.startSequence = decodeLong(initialFrame.content, REQUEST_START_SEQUENCE_FIELD_OFFSET);
        request.minSize = decodeInt(initialFrame.content, REQUEST_MIN_SIZE_FIELD_OFFSET);
        request.maxSize = decodeInt(initialFrame.content, REQUEST_MAX_SIZE_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.predicate = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        request.projection = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public int readCount;

        /**
         * TODO DOC
         */
        public java.util.List<com.hazelcast.nio.serialization.Data> items;

        /**
         * TODO DOC
         */
        public long[] itemSeqs;

        /**
         * TODO DOC
         */
        public long nextSeq;
    }

    public static ClientMessage encodeResponse(int readCount, java.util.Collection<com.hazelcast.nio.serialization.Data> items, long[] itemSeqs, long nextSeq) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeInt(initialFrame.content, RESPONSE_READ_COUNT_FIELD_OFFSET, readCount);
        encodeLong(initialFrame.content, RESPONSE_NEXT_SEQ_FIELD_OFFSET, nextSeq);
        ListMultiFrameCodec.encode(clientMessage, items, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, itemSeqs, LongArrayCodec::encode);
        return clientMessage;
    }

    public static CacheEventJournalReadCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.readCount = decodeInt(initialFrame.content, RESPONSE_READ_COUNT_FIELD_OFFSET);
        response.nextSeq = decodeLong(initialFrame.content, RESPONSE_NEXT_SEQ_FIELD_OFFSET);
        response.items = ListMultiFrameCodec.decode(iterator, DataCodec::decode);
        response.itemSeqs = CodecUtil.decodeNullable(iterator, LongArrayCodec::decode);
        return response;
    }

}

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
 * Performs the initial subscription to the cache event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 */
public class CacheEventJournalSubscribeCodec {

        private static final int REQUEST_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 5409;//hex: 0x1521,
        private static final int RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        private static final int RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET = RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 125;//hex: 0x007D,

    public static class RequestParameters {

        /**
         * name of the cache
         */
        public java.lang.String name;
    }

    public static ClientMessage encodeRequest(java.lang.String name) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Cache.EventJournalSubscribe");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CacheEventJournalSubscribeCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        iterator.next();//empty initial frame
        request.name = StringCodec.decode(iterator);
        return request;
    }

    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public long oldestSequence;

        /**
         * TODO DOC
         */
        public long newestSequence;
    }

    public static ClientMessage encodeResponse(long oldestSequence, long newestSequence) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        encodeLong(initialFrame.content, RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET, oldestSequence);
        encodeLong(initialFrame.content, RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET, newestSequence);
        return clientMessage;
    }

    public static CacheEventJournalSubscribeCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.oldestSequence = decodeLong(initialFrame.content, RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET);
        response.newestSequence = decodeLong(initialFrame.content, RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET);
        return response;
    }

}
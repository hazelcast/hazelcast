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
 * Performs the initial subscription to the map event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 */
@Generated("70fa63c350b68d3dafec548174805e38")
public final class MapEventJournalSubscribeCodec {
    //hex: 0x014100
    public static final int REQUEST_MESSAGE_TYPE = 82176;
    //hex: 0x014101
    public static final int RESPONSE_MESSAGE_TYPE = 82177;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET = RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private MapEventJournalSubscribeCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of the map
         */
        public java.lang.String name;
    }

    public static ClientMessage encodeRequest(java.lang.String name) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("Map.EventJournalSubscribe");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static MapEventJournalSubscribeCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
            /**
             * Sequence id of the oldest event in the event journal.
             */
            public long oldestSequence;
            /**
             * Sequence id of the newest event in the event journal.
             */
            public long newestSequence;

    }

    public static ClientMessage encodeResponse(long oldestSequence, long newestSequence) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeLong(initialFrame.content, RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET, oldestSequence);
        encodeLong(initialFrame.content, RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET, newestSequence);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static MapEventJournalSubscribeCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.oldestSequence = decodeLong(initialFrame.content, RESPONSE_OLDEST_SEQUENCE_FIELD_OFFSET);
        response.newestSequence = decodeLong(initialFrame.content, RESPONSE_NEWEST_SEQUENCE_FIELD_OFFSET);
        return response;
    }

}


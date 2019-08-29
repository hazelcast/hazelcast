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
 * Creates a session for the caller on the given CP group.
 */
public final class CPSessionCreateSessionCodec {
    //hex: 0x220100
    public static final int REQUEST_MESSAGE_TYPE = 2228480;
    //hex: 0x220101
    public static final int RESPONSE_MESSAGE_TYPE = 2228481;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_SESSION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_TTL_MILLIS_FIELD_OFFSET = RESPONSE_SESSION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_HEARTBEAT_MILLIS_FIELD_OFFSET = RESPONSE_TTL_MILLIS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_HEARTBEAT_MILLIS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private CPSessionCreateSessionCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * ID of the CP group
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * Name of the caller HazelcastInstance
         */
        public java.lang.String endpointName;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String endpointName) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("CPSession.CreateSession");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, endpointName);
        return clientMessage;
    }

    public static CPSessionCreateSessionCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.endpointName = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public long sessionId;

        /**
         * TODO DOC
         */
        public long ttlMillis;

        /**
         * TODO DOC
         */
        public long heartbeatMillis;
    }

    public static ClientMessage encodeResponse(long sessionId, long ttlMillis, long heartbeatMillis) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeLong(initialFrame.content, RESPONSE_SESSION_ID_FIELD_OFFSET, sessionId);
        encodeLong(initialFrame.content, RESPONSE_TTL_MILLIS_FIELD_OFFSET, ttlMillis);
        encodeLong(initialFrame.content, RESPONSE_HEARTBEAT_MILLIS_FIELD_OFFSET, heartbeatMillis);
        return clientMessage;
    }

    public static CPSessionCreateSessionCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.sessionId = decodeLong(initialFrame.content, RESPONSE_SESSION_ID_FIELD_OFFSET);
        response.ttlMillis = decodeLong(initialFrame.content, RESPONSE_TTL_MILLIS_FIELD_OFFSET);
        response.heartbeatMillis = decodeLong(initialFrame.content, RESPONSE_HEARTBEAT_MILLIS_FIELD_OFFSET);
        return response;
    }

}

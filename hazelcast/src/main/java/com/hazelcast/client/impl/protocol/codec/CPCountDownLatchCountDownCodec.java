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
 * Decrements the count of the latch, releasing all waiting threads if
 * the count reaches zero. If the current count is greater than zero, then
 * it is decremented. If the new count is zero: All waiting threads are
 * re-enabled for thread scheduling purposes, and Countdown owner is set to
 * null. If the current count equals zero, then nothing happens.
 */
public final class CPCountDownLatchCountDownCodec {
    //hex: 0x250300
    public static final int REQUEST_MESSAGE_TYPE = 2425600;
    //hex: 0x250301
    public static final int RESPONSE_MESSAGE_TYPE = 2425601;
    private static final int REQUEST_INVOCATION_UID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_EXPECTED_ROUND_FIELD_OFFSET = REQUEST_INVOCATION_UID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_EXPECTED_ROUND_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private CPCountDownLatchCountDownCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * CP group id of this CountDownLatch instance
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * Name of the CountDownLatch instance
         */
        public java.lang.String name;

        /**
         * UID of this invocation
         */
        public java.util.UUID invocationUid;

        /**
         * The round this invocation will be performed on
         */
        public int expectedRound;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name, java.util.UUID invocationUid, int expectedRound) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("CPCountDownLatch.CountDown");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeUUID(initialFrame.content, REQUEST_INVOCATION_UID_FIELD_OFFSET, invocationUid);
        encodeInt(initialFrame.content, REQUEST_EXPECTED_ROUND_FIELD_OFFSET, expectedRound);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CPCountDownLatchCountDownCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.invocationUid = decodeUUID(initialFrame.content, REQUEST_INVOCATION_UID_FIELD_OFFSET);
        request.expectedRound = decodeInt(initialFrame.content, REQUEST_EXPECTED_ROUND_FIELD_OFFSET);
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static CPCountDownLatchCountDownCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}

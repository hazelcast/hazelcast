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
 * Returns current lock ownership status of the given FencedLock instance.
 */
public final class CPFencedLockGetLockOwnershipCodec {
    //hex: 0x260400
    public static final int REQUEST_MESSAGE_TYPE = 2491392;
    //hex: 0x260401
    public static final int RESPONSE_MESSAGE_TYPE = 2491393;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_FENCE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_LOCK_COUNT_FIELD_OFFSET = RESPONSE_FENCE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_SESSION_ID_FIELD_OFFSET = RESPONSE_LOCK_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_THREAD_ID_FIELD_OFFSET = RESPONSE_SESSION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_THREAD_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private CPFencedLockGetLockOwnershipCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * CP group id of this FencedLock instance
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * Name of this FencedLock instance
         */
        public java.lang.String name;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("CPFencedLock.GetLockOwnership");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CPFencedLockGetLockOwnershipCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * TODO DOC
         */
        public long fence;

        /**
         * TODO DOC
         */
        public int lockCount;

        /**
         * TODO DOC
         */
        public long sessionId;

        /**
         * TODO DOC
         */
        public long threadId;
    }

    public static ClientMessage encodeResponse(long fence, int lockCount, long sessionId, long threadId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeLong(initialFrame.content, RESPONSE_FENCE_FIELD_OFFSET, fence);
        encodeInt(initialFrame.content, RESPONSE_LOCK_COUNT_FIELD_OFFSET, lockCount);
        encodeLong(initialFrame.content, RESPONSE_SESSION_ID_FIELD_OFFSET, sessionId);
        encodeLong(initialFrame.content, RESPONSE_THREAD_ID_FIELD_OFFSET, threadId);
        return clientMessage;
    }

    public static CPFencedLockGetLockOwnershipCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.fence = decodeLong(initialFrame.content, RESPONSE_FENCE_FIELD_OFFSET);
        response.lockCount = decodeInt(initialFrame.content, RESPONSE_LOCK_COUNT_FIELD_OFFSET);
        response.sessionId = decodeLong(initialFrame.content, RESPONSE_SESSION_ID_FIELD_OFFSET);
        response.threadId = decodeLong(initialFrame.content, RESPONSE_THREAD_ID_FIELD_OFFSET);
        return response;
    }

}

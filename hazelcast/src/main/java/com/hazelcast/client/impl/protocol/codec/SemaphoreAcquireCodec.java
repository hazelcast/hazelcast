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
 * Acquires the requested amount of permits if available, reducing
 * the number of available permits. If no enough permits are available,
 * then the current thread becomes disabled for thread scheduling purposes
 * and lies dormant until other threads release enough permits.
 */
@Generated("5ed6af77ddfcc5fedef9bf5b67e6035a")
public final class SemaphoreAcquireCodec {
    //hex: 0x0C0200
    public static final int REQUEST_MESSAGE_TYPE = 786944;
    //hex: 0x0C0201
    public static final int RESPONSE_MESSAGE_TYPE = 786945;
    private static final int REQUEST_SESSION_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_THREAD_ID_FIELD_OFFSET = REQUEST_SESSION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INVOCATION_UID_FIELD_OFFSET = REQUEST_THREAD_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_PERMITS_FIELD_OFFSET = REQUEST_INVOCATION_UID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int REQUEST_TIMEOUT_MS_FIELD_OFFSET = REQUEST_PERMITS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_TIMEOUT_MS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SemaphoreAcquireCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * CP group id of this ISemaphore instance
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * Name of this ISemaphore instance
         */
        public java.lang.String name;

        /**
         * Session ID of the caller
         */
        public long sessionId;

        /**
         * ID of the caller thread
         */
        public long threadId;

        /**
         * UID of this invocation
         */
        public java.util.UUID invocationUid;

        /**
         * number of permits to acquire
         */
        public int permits;

        /**
         * Duration to wait for permit acquire
         */
        public long timeoutMs;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name, long sessionId, long threadId, java.util.UUID invocationUid, int permits, long timeoutMs) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("Semaphore.Acquire");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_SESSION_ID_FIELD_OFFSET, sessionId);
        encodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET, threadId);
        encodeUUID(initialFrame.content, REQUEST_INVOCATION_UID_FIELD_OFFSET, invocationUid);
        encodeInt(initialFrame.content, REQUEST_PERMITS_FIELD_OFFSET, permits);
        encodeLong(initialFrame.content, REQUEST_TIMEOUT_MS_FIELD_OFFSET, timeoutMs);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static SemaphoreAcquireCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.sessionId = decodeLong(initialFrame.content, REQUEST_SESSION_ID_FIELD_OFFSET);
        request.threadId = decodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET);
        request.invocationUid = decodeUUID(initialFrame.content, REQUEST_INVOCATION_UID_FIELD_OFFSET);
        request.permits = decodeInt(initialFrame.content, REQUEST_PERMITS_FIELD_OFFSET);
        request.timeoutMs = decodeLong(initialFrame.content, REQUEST_TIMEOUT_MS_FIELD_OFFSET);
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    /**
     * true if requested permits are acquired,
     * false otherwise
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD"})
    public boolean response;

    public static ClientMessage encodeResponse(boolean response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static boolean decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

}


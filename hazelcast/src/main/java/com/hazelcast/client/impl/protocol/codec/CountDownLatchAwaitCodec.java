/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
 * Causes the current thread to wait until the latch has counted down
 * to zero, or an exception is thrown, or the specified waiting time
 * elapses. If the current count is zero then this method returns
 * immediately with the value true. If the current count is greater than
 * zero, then the current thread becomes disabled for thread scheduling
 * purposes and lies dormant until one of five things happen: the count
 * reaches zero due to invocations of the {@code countDown} method, this
 * ICountDownLatch instance is destroyed, the countdown owner becomes
 * disconnected, some other thread Thread#interrupt interrupts the current
 * thread, or the specified waiting time elapses. If the count reaches zero
 * then the method returns with the value true. If the current thread has
 * its interrupted status set on entry to this method, or is interrupted
 * while waiting, then {@code InterruptedException} is thrown
 * and the current thread's interrupted status is cleared. If the specified
 * waiting time elapses then the value false is returned.  If the time is
 * less than or equal to zero, the method will not wait at all.
 */
@SuppressWarnings("unused")
@Generated("de0b6d9802da3abffd0943941e293011")
public final class CountDownLatchAwaitCodec {
    //hex: 0x0B0200
    public static final int REQUEST_MESSAGE_TYPE = 721408;
    //hex: 0x0B0201
    public static final int RESPONSE_MESSAGE_TYPE = 721409;
    private static final int REQUEST_INVOCATION_UID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_TIMEOUT_MS_FIELD_OFFSET = REQUEST_INVOCATION_UID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_TIMEOUT_MS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private CountDownLatchAwaitCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * CP group id of this CountDownLatch instance
         */
        public com.hazelcast.cp.internal.RaftGroupId groupId;

        /**
         * Name of this CountDownLatch instance
         */
        public java.lang.String name;

        /**
         * UID of this invocation
         */
        public java.util.UUID invocationUid;

        /**
         * The maximum time in milliseconds to wait
         */
        public long timeoutMs;
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name, java.util.UUID invocationUid, long timeoutMs) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("CountDownLatch.Await");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeUUID(initialFrame.content, REQUEST_INVOCATION_UID_FIELD_OFFSET, invocationUid);
        encodeLong(initialFrame.content, REQUEST_TIMEOUT_MS_FIELD_OFFSET, timeoutMs);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CountDownLatchAwaitCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.invocationUid = decodeUUID(initialFrame.content, REQUEST_INVOCATION_UID_FIELD_OFFSET);
        request.timeoutMs = decodeLong(initialFrame.content, REQUEST_TIMEOUT_MS_FIELD_OFFSET);
        request.groupId = RaftGroupIdCodec.decode(iterator);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    public static ClientMessage encodeResponse(boolean response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    /**
     * true if the count reached zero, false if
     * the waiting time elapsed before the count reached 0
     */
    public static boolean decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }
}

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * Cancels the task running on the member with the given address.
 */
@Generated("7809388a7e14be60986cb995144a7af7")
public final class ExecutorServiceCancelOnMemberCodec {
    //hex: 0x080400
    public static final int REQUEST_MESSAGE_TYPE = 525312;
    //hex: 0x080401
    public static final int RESPONSE_MESSAGE_TYPE = 525313;
    private static final int REQUEST_UUID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MEMBER_UUID_FIELD_OFFSET = REQUEST_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int REQUEST_INTERRUPT_FIELD_OFFSET = REQUEST_MEMBER_UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_INTERRUPT_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private ExecutorServiceCancelOnMemberCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Unique id for the execution.
         */
        public java.util.UUID uuid;

        /**
         * The UUID of the member to execute the request on.
         */
        public java.util.UUID memberUUID;

        /**
         * If true, then the thread interrupt call can be used to cancel the thread, otherwise interrupt can not be used.
         */
        public boolean interrupt;
    }

    public static ClientMessage encodeRequest(java.util.UUID uuid, java.util.UUID memberUUID, boolean interrupt) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("ExecutorService.CancelOnMember");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeUUID(initialFrame.content, REQUEST_UUID_FIELD_OFFSET, uuid);
        encodeUUID(initialFrame.content, REQUEST_MEMBER_UUID_FIELD_OFFSET, memberUUID);
        encodeBoolean(initialFrame.content, REQUEST_INTERRUPT_FIELD_OFFSET, interrupt);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static ExecutorServiceCancelOnMemberCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.uuid = decodeUUID(initialFrame.content, REQUEST_UUID_FIELD_OFFSET);
        request.memberUUID = decodeUUID(initialFrame.content, REQUEST_MEMBER_UUID_FIELD_OFFSET);
        request.interrupt = decodeBoolean(initialFrame.content, REQUEST_INTERRUPT_FIELD_OFFSET);
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
     * True if cancelled successfully, false otherwise.
     */
    public static boolean decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        return decodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }
}

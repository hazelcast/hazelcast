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
 * Acquires the lock for the specified key for the specified lease time. After the lease time, the lock will be
 * released. If the lock is not available, then the current thread becomes disabled for thread scheduling
 * purposes and lies dormant until the lock has been acquired. Scope of the lock is for this map only. The acquired
 * lock is only for the key in this map.Locks are re-entrant, so if the key is locked N times, then it should be
 * unlocked N times before another thread can acquire it.
 */
@Generated("d4069a070c6df41a78a900e912f822d2")
public final class MultiMapLockCodec {
    //hex: 0x021000
    public static final int REQUEST_MESSAGE_TYPE = 135168;
    //hex: 0x021001
    public static final int RESPONSE_MESSAGE_TYPE = 135169;
    private static final int REQUEST_THREAD_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_TTL_FIELD_OFFSET = REQUEST_THREAD_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_REFERENCE_ID_FIELD_OFFSET = REQUEST_TTL_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_REFERENCE_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private MultiMapLockCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the MultiMap
         */
        public java.lang.String name;

        /**
         * The key the Lock
         */
        public com.hazelcast.internal.serialization.Data key;

        /**
         * The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
         */
        public long threadId;

        /**
         * The duration in milliseconds after which this entry shall be deleted. O means infinite.
         */
        public long ttl;

        /**
         * The client-wide unique id for this request. It is used to make the request idempotent by sending the same reference id during retries.
         */
        public long referenceId;
    }

    public static ClientMessage encodeRequest(java.lang.String name, com.hazelcast.internal.serialization.Data key, long threadId, long ttl, long referenceId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("MultiMap.Lock");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET, threadId);
        encodeLong(initialFrame.content, REQUEST_TTL_FIELD_OFFSET, ttl);
        encodeLong(initialFrame.content, REQUEST_REFERENCE_ID_FIELD_OFFSET, referenceId);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, key);
        return clientMessage;
    }

    public static MultiMapLockCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.threadId = decodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET);
        request.ttl = decodeLong(initialFrame.content, REQUEST_TTL_FIELD_OFFSET);
        request.referenceId = decodeLong(initialFrame.content, REQUEST_REFERENCE_ID_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.key = DataCodec.decode(iterator);
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

    public static MultiMapLockCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}

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

package com.hazelcast.internal.longregister.client.codec;

import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.ClientMessage.CORRELATION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.PARTITION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.INT_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

/**
 * Atomically increments the current value by one.
 */
@Generated("6fec9ba7edddc4031ccf88dee0837b6f")
public final class LongRegisterIncrementAndGetCodec {
    //hex: 0xFF0700
    public static final int REQUEST_MESSAGE_TYPE = 16713472;
    //hex: 0xFF0701
    public static final int RESPONSE_MESSAGE_TYPE = 16713473;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private LongRegisterIncrementAndGetCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * The name of this IAtomicLong instance.
         */
        public java.lang.String name;
    }

    public static ClientMessage encodeRequest(java.lang.String name) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("LongRegister.IncrementAndGet");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static LongRegisterIncrementAndGetCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
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
         * The updated value, the current value incremented by one
         */
        public long response;
    }

    public static ClientMessage encodeResponse(long response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        return clientMessage;
    }

    public static LongRegisterIncrementAndGetCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.response = decodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
        return response;
    }

}

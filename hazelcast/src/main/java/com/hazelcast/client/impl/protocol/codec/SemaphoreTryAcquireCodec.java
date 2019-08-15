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
 * Acquires the given number of permits, if they are available, and returns immediately, with the value true,
 * reducing the number of available permits by the given amount. If insufficient permits are available then this
 * method will return immediately with the value false and the number of available permits is unchanged.
 */
public final class SemaphoreTryAcquireCodec {
    //hex: 0x0D0700
    public static final int REQUEST_MESSAGE_TYPE = 853760;
    //hex: 0x0D0701
    public static final int RESPONSE_MESSAGE_TYPE = 853761;
    private static final int REQUEST_PERMITS_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_TIMEOUT_FIELD_OFFSET = REQUEST_PERMITS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_TIMEOUT_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SemaphoreTryAcquireCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Semaphore
         */
        public java.lang.String name;

        /**
         * The number of permits to remove
         */
        public int permits;

        /**
         * The maximum time to wait for a permit
         */
        public long timeout;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int permits, long timeout) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(true);
        clientMessage.setOperationName("Semaphore.TryAcquire");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_PERMITS_FIELD_OFFSET, permits);
        encodeLong(initialFrame.content, REQUEST_TIMEOUT_FIELD_OFFSET, timeout);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static SemaphoreTryAcquireCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.permits = decodeInt(initialFrame.content, REQUEST_PERMITS_FIELD_OFFSET);
        request.timeout = decodeLong(initialFrame.content, REQUEST_TIMEOUT_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * true if all permits were acquired,  false if the waiting time elapsed before all permits could be acquired
         */
        public boolean response;
    }

    public static ClientMessage encodeResponse(boolean response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        return clientMessage;
    }

    public static SemaphoreTryAcquireCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.response = decodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
        return response;
    }

}

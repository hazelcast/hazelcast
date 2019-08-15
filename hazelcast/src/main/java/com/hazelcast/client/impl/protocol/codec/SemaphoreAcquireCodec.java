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
 * Acquires the given number of permits if they are available, and returns immediately, reducing the number of
 * available permits by the given amount. If insufficient permits are available then the current thread becomes
 * disabled for thread scheduling purposes and lies dormant until one of three things happens: some other thread
 * invokes one of the methods for this semaphore, the current thread is next to be assigned permits and the number
 * of available permits satisfies this request, this ISemaphore instance is destroyed, or some other thread
 * the current thread. If the current thread has its interrupted status set on entry to this method, or is  while
 * waiting for a permit, then  is thrown and the current thread's interrupted status is cleared.
 */
public final class SemaphoreAcquireCodec {
    //hex: 0x0D0200
    public static final int REQUEST_MESSAGE_TYPE = 852480;
    //hex: 0x0D0201
    public static final int RESPONSE_MESSAGE_TYPE = 852481;
    private static final int REQUEST_PERMITS_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_PERMITS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private SemaphoreAcquireCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Semaphore
         */
        public java.lang.String name;

        /**
         * The given permit count
         */
        public int permits;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int permits) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(true);
        clientMessage.setOperationName("Semaphore.Acquire");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_PERMITS_FIELD_OFFSET, permits);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static SemaphoreAcquireCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.permits = decodeInt(initialFrame.content, REQUEST_PERMITS_FIELD_OFFSET);
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

    public static SemaphoreAcquireCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}

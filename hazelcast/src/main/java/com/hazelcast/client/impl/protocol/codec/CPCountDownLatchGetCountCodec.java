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
 * Returns the current count.
 */
public final class CPCountDownLatchGetCountCodec {
    //hex: 0x250400
    public static final int REQUEST_MESSAGE_TYPE = 2425856;
    //hex: 0x250401
    public static final int RESPONSE_MESSAGE_TYPE = 2425857;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private CPCountDownLatchGetCountCodec() {
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
    }

    public static ClientMessage encodeRequest(com.hazelcast.cp.internal.RaftGroupId groupId, java.lang.String name) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("CPCountDownLatch.GetCount");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        RaftGroupIdCodec.encode(clientMessage, groupId);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static CPCountDownLatchGetCountCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
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
         * The current count of this CountDownLatch instance
         */
        public int response;
    }

    public static ClientMessage encodeResponse(int response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        encodeInt(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        return clientMessage;
    }

    public static CPCountDownLatchGetCountCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.response = decodeInt(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
        return response;
    }

}

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
 * Updates TTL (time to live) value of the entry specified by {@code key} with a new TTL value.
 * New TTL value is valid from this operation is invoked, not from the original creation of the entry.
 * If the entry does not exist or already expired, then this call has no effect.
 * <p>
 * The entry will expire and get evicted after the TTL. If the TTL is 0,
 * then the entry lives forever. If the TTL is negative, then the TTL
 * from the map configuration will be used (default: forever).
 * 
 * If there is no entry with key {@code key}, this call has no effect.
 * 
 * <b>Warning:</b>
 * <p>
 * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
 */
@Generated("d960d973d10979d22674baa8278ebbe8")
public final class MapSetTtlCodec {
    //hex: 0x014500
    public static final int REQUEST_MESSAGE_TYPE = 83200;
    //hex: 0x014501
    public static final int RESPONSE_MESSAGE_TYPE = 83201;
    private static final int REQUEST_TTL_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_TTL_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private MapSetTtlCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the map
         */
        public java.lang.String name;

        /**
         * Key for the map entry
         */
        public com.hazelcast.nio.serialization.Data key;

        /**
         * The duration in milliseconds after which this entry shall be deleted. O means infinite.
         */
        public long ttl;
    }

    public static ClientMessage encodeRequest(java.lang.String name, com.hazelcast.nio.serialization.Data key, long ttl) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Map.SetTtl");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeLong(initialFrame.content, REQUEST_TTL_FIELD_OFFSET, ttl);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, key);
        return clientMessage;
    }

    public static MapSetTtlCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.ttl = decodeLong(initialFrame.content, REQUEST_TTL_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.key = DataCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * 'true' if the entry is affected, 'false' otherwise
         */
        public boolean response;
    }

    public static ClientMessage encodeResponse(boolean response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static MapSetTtlCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        response.response = decodeBoolean(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
        return response;
    }

}

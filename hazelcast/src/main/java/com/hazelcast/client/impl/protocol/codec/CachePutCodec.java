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
import com.hazelcast.internal.serialization.Data;

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
 * TODO DOC
 */
@Generated("e9257d0b596ab1aaf6675d6d1da1f7db")
public final class CachePutCodec {
    //hex: 0x131300
    public static final int REQUEST_MESSAGE_TYPE = 1250048;
    //hex: 0x131301
    public static final int RESPONSE_MESSAGE_TYPE = 1250049;
    private static final int REQUEST_GET_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_COMPLETION_ID_FIELD_OFFSET = REQUEST_GET_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_COMPLETION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private CachePutCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the cache.
         */
        public java.lang.String name;

        /**
         * The key that has the specified value associated with it.
         */
        public Data key;

        /**
         * The value to be associated with the key.
         */
        public Data value;

        /**
         * Expiry policy for the entry. Byte-array which is serialized from an object implementing
         * javax.cache.expiry.ExpiryPolicy interface.
         */
        public @Nullable
        Data expiryPolicy;

        /**
         * boolean flag indicating if the previous value should be retrieved.
         */
        public boolean get;

        /**
         * User generated id which shall be received as a field of the cache event upon completion of
         * the request in the cluster.
         */
        public int completionId;
    }

    public static ClientMessage encodeRequest(java.lang.String name, Data key, Data value, @Nullable Data expiryPolicy, boolean get, int completionId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Cache.Put");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_GET_FIELD_OFFSET, get);
        encodeInt(initialFrame.content, REQUEST_COMPLETION_ID_FIELD_OFFSET, completionId);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, key);
        DataCodec.encode(clientMessage, value);
        CodecUtil.encodeNullable(clientMessage, expiryPolicy, DataCodec::encode);
        return clientMessage;
    }

    public static CachePutCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.get = decodeBoolean(initialFrame.content, REQUEST_GET_FIELD_OFFSET);
        request.completionId = decodeInt(initialFrame.content, REQUEST_COMPLETION_ID_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.key = DataCodec.decode(iterator);
        request.value = DataCodec.decode(iterator);
        request.expiryPolicy = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * The value previously assigned to the given key, or null if not assigned.
         */
        public @Nullable
        Data response;
    }

    public static ClientMessage encodeResponse(@Nullable Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, response, DataCodec::encode);
        return clientMessage;
    }

    public static CachePutCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.response = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        return response;
    }

}

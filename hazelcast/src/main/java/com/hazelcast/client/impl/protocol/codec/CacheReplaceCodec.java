/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
 * Atomically replaces the currently assigned value for the given key with the specified newValue if and only if the
 * currently assigned value equals the value of oldValue using a custom javax.cache.expiry.ExpiryPolicy
 * If the cache is configured for write-through operation mode, the underlying configured
 * javax.cache.integration.CacheWriter might be called to store the value of the key to any kind of external resource.
 */
@SuppressWarnings("unused")
@Generated("b1956908eeade5876a6369584cdefc8e")
public final class CacheReplaceCodec {
    //hex: 0x131700
    public static final int REQUEST_MESSAGE_TYPE = 1251072;
    //hex: 0x131701
    public static final int RESPONSE_MESSAGE_TYPE = 1251073;
    private static final int REQUEST_COMPLETION_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_COMPLETION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private CacheReplaceCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the cache.
         */
        public java.lang.String name;

        /**
         * The key whose value is replaced.
         */
        public com.hazelcast.internal.serialization.Data key;

        /**
         * Old value to match if exists before removing. Null means "don't try to remove"
         */
        public @Nullable com.hazelcast.internal.serialization.Data oldValue;

        /**
         * The new value to be associated with the specified key.
         */
        public com.hazelcast.internal.serialization.Data newValue;

        /**
         * Expiry policy for the entry. Byte-array which is serialized from an object implementing
         * javax.cache.expiry.ExpiryPolicy interface.
         */
        public @Nullable com.hazelcast.internal.serialization.Data expiryPolicy;

        /**
         * User generated id which shall be received as a field of the cache event upon completion of
         * the request in the cluster.
         */
        public int completionId;
    }

    public static ClientMessage encodeRequest(java.lang.String name, com.hazelcast.internal.serialization.Data key, @Nullable com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data newValue, @Nullable com.hazelcast.internal.serialization.Data expiryPolicy, int completionId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Cache.Replace");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_COMPLETION_ID_FIELD_OFFSET, completionId);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        DataCodec.encode(clientMessage, key);
        CodecUtil.encodeNullable(clientMessage, oldValue, DataCodec::encode);
        DataCodec.encode(clientMessage, newValue);
        CodecUtil.encodeNullable(clientMessage, expiryPolicy, DataCodec::encode);
        return clientMessage;
    }

    public static CacheReplaceCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.completionId = decodeInt(initialFrame.content, REQUEST_COMPLETION_ID_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.key = DataCodec.decode(iterator);
        request.oldValue = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        request.newValue = DataCodec.decode(iterator);
        request.expiryPolicy = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        return request;
    }

    public static ClientMessage encodeResponse(@Nullable com.hazelcast.internal.serialization.Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, response, DataCodec::encode);
        return clientMessage;
    }

    /**
     * The replaced value.
     */
    public static com.hazelcast.internal.serialization.Data decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        //empty initial frame
        iterator.next();
        return CodecUtil.decodeNullable(iterator, DataCodec::decode);
    }
}

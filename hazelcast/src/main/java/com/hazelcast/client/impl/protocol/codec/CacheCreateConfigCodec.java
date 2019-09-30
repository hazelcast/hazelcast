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

import java.util.ListIterator;

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
@Generated("a52b502d22c095c57028df7d25d93ca4")
public final class CacheCreateConfigCodec {
    //hex: 0x150700
    public static final int REQUEST_MESSAGE_TYPE = 1378048;
    //hex: 0x150701
    public static final int RESPONSE_MESSAGE_TYPE = 1378049;
    private static final int REQUEST_CREATE_ALSO_ON_OTHERS_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_CREATE_ALSO_ON_OTHERS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private CacheCreateConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * The cache configuration.
         */
        public com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder cacheConfig;

        /**
         * True if the configuration shall be created on all members, false otherwise.
         */
        public boolean createAlsoOnOthers;
    }

    public static ClientMessage encodeRequest(com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder cacheConfig, boolean createAlsoOnOthers) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Cache.CreateConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_CREATE_ALSO_ON_OTHERS_FIELD_OFFSET, createAlsoOnOthers);
        clientMessage.add(initialFrame);
        CacheConfigHolderCodec.encode(clientMessage, cacheConfig);
        return clientMessage;
    }

    public static CacheCreateConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.createAlsoOnOthers = decodeBoolean(initialFrame.content, REQUEST_CREATE_ALSO_ON_OTHERS_FIELD_OFFSET);
        request.cacheConfig = CacheConfigHolderCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {

        /**
         * The created configuration object.
         */
        public com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder response;
    }

    public static ClientMessage encodeResponse(com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, response, CacheConfigHolderCodec::encode);
        return clientMessage;
    }

    public static CacheCreateConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        response.response = CodecUtil.decodeNullable(iterator, CacheConfigHolderCodec::decode);
        return response;
    }

}

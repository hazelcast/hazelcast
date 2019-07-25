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
 * Adds a new merkle tree configuration to a running cluster.
 * If a merkle tree configuration with the given {@code name} already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
public class DynamicConfigAddMerkleTreeConfigCodec {

        private static final int REQUEST_ENABLED_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
        private static final int REQUEST_DEPTH_FIELD_OFFSET = REQUEST_ENABLED_FIELD_OFFSET + INT_SIZE_IN_BYTES;
        private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_DEPTH_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int REQUEST_MESSAGE_TYPE = 7703;//hex: 0x1E17,
        private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
        public static final int RESPONSE_MESSAGE_TYPE = 100;//hex: 0x0064,

    public static class RequestParameters {

        /**
         * map name to which this config applies. Map names
         * are also matched by pattern and a merkle with map name "default"
         * applies to all maps that do not have more specific merkle tree configs.
         */
        public java.lang.String mapName;

        /**
         * {@code true} to enable this merkle tree configuration, otherwise {@code false}
         */
        public boolean enabled;

        /**
         * depth of the merkle tree. The depth must be between
         * {@value com.hazelcast.config.MerkleTreeConfig#MIN_DEPTH}
         * and {@value com.hazelcast.config.MerkleTreeConfig#MAX_DEPTH} (exclusive).
         */
        public int depth;
    }

    public static ClientMessage encodeRequest(java.lang.String mapName, boolean enabled, int depth) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("DynamicConfig.AddMerkleTreeConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_ENABLED_FIELD_OFFSET, enabled);
        encodeInt(initialFrame.content, REQUEST_DEPTH_FIELD_OFFSET, depth);
        clientMessage.addFrame(initialFrame);
        StringCodec.encode(clientMessage, mapName);
        return clientMessage;
    }

    public static DynamicConfigAddMerkleTreeConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.enabled = decodeBoolean(initialFrame.content, REQUEST_ENABLED_FIELD_OFFSET);
        request.depth = decodeInt(initialFrame.content, REQUEST_DEPTH_FIELD_OFFSET);
        request.mapName = StringCodec.decode(iterator);
        return request;
    }

    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.addFrame(initialFrame);

        return clientMessage;
    }

    public static DynamicConfigAddMerkleTreeConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.iterator();
        ResponseParameters response = new ResponseParameters();
        iterator.next();//empty initial frame
        return response;
    }

}
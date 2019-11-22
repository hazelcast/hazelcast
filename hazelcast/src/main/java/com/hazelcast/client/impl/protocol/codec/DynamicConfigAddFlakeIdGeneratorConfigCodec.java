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
 * Adds a new flake ID generator configuration to a running cluster.
 * If a flake ID generator configuration for the same name already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
@Generated("f998bdef9b5d75f20d026bfa302fce1a")
public final class DynamicConfigAddFlakeIdGeneratorConfigCodec {
    //hex: 0x1B0F00
    public static final int REQUEST_MESSAGE_TYPE = 1773312;
    //hex: 0x1B0F01
    public static final int RESPONSE_MESSAGE_TYPE = 1773313;
    private static final int REQUEST_PREFETCH_COUNT_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_PREFETCH_VALIDITY_FIELD_OFFSET = REQUEST_PREFETCH_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_ID_OFFSET_FIELD_OFFSET = REQUEST_PREFETCH_VALIDITY_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_STATISTICS_ENABLED_FIELD_OFFSET = REQUEST_ID_OFFSET_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_NODE_ID_OFFSET_FIELD_OFFSET = REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_NODE_ID_OFFSET_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private DynamicConfigAddFlakeIdGeneratorConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of {@code FlakeIdGenerator}
         */
        public java.lang.String name;

        /**
         * how many IDs are pre-fetched on the background when one call to {@code newId()} is made
         */
        public int prefetchCount;

        /**
         * for how long the pre-fetched IDs can be used
         */
        public long prefetchValidity;

        /**
         * TODO DOC
         */
        public long idOffset;

        /**
         * {@code true} to enable gathering of statistics, otherwise {@code false}
         */
        public boolean statisticsEnabled;

        /**
         * TODO DOC
         */
        public long nodeIdOffset;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int prefetchCount, long prefetchValidity, long idOffset, boolean statisticsEnabled, long nodeIdOffset) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("DynamicConfig.AddFlakeIdGeneratorConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_PREFETCH_COUNT_FIELD_OFFSET, prefetchCount);
        encodeLong(initialFrame.content, REQUEST_PREFETCH_VALIDITY_FIELD_OFFSET, prefetchValidity);
        encodeLong(initialFrame.content, REQUEST_ID_OFFSET_FIELD_OFFSET, idOffset);
        encodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET, statisticsEnabled);
        encodeLong(initialFrame.content, REQUEST_NODE_ID_OFFSET_FIELD_OFFSET, nodeIdOffset);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    public static DynamicConfigAddFlakeIdGeneratorConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.prefetchCount = decodeInt(initialFrame.content, REQUEST_PREFETCH_COUNT_FIELD_OFFSET);
        request.prefetchValidity = decodeLong(initialFrame.content, REQUEST_PREFETCH_VALIDITY_FIELD_OFFSET);
        request.idOffset = decodeLong(initialFrame.content, REQUEST_ID_OFFSET_FIELD_OFFSET);
        request.statisticsEnabled = decodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET);
        request.nodeIdOffset = decodeLong(initialFrame.content, REQUEST_NODE_ID_OFFSET_FIELD_OFFSET);
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

    public static DynamicConfigAddFlakeIdGeneratorConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}

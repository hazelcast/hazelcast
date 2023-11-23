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
 * Adds a WAN replication configuration.
 */
@SuppressWarnings("unused")
@Generated("60993b49fb05b81dfd3f162720d14b4d")
public final class DynamicConfigAddWanReplicationConfigCodec {
    //hex: 0x1B1200
    public static final int REQUEST_MESSAGE_TYPE = 1774080;
    //hex: 0x1B1201
    public static final int RESPONSE_MESSAGE_TYPE = 1774081;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private DynamicConfigAddWanReplicationConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * WAN replication configuration name.
         */
        public java.lang.String name;

        /**
         * The WAN consumer configuration.
         */
        public @Nullable com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder consumerConfig;

        /**
         * The WAN custom publisher configurations.
         */
        public java.util.List<com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder> customPublisherConfigs;

        /**
         * The WAN batch publisher configurations.
         */
        public java.util.List<com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder> batchPublisherConfigs;
    }

    public static ClientMessage encodeRequest(java.lang.String name, @Nullable com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder consumerConfig, java.util.List<com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder> customPublisherConfigs, java.util.List<com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder> batchPublisherConfigs) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("DynamicConfig.AddWanReplicationConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        CodecUtil.encodeNullable(clientMessage, consumerConfig, WanConsumerConfigHolderCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, customPublisherConfigs, WanCustomPublisherConfigHolderCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, batchPublisherConfigs, WanBatchPublisherConfigHolderCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddWanReplicationConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.name = StringCodec.decode(iterator);
        request.consumerConfig = CodecUtil.decodeNullable(iterator, WanConsumerConfigHolderCodec::decode);
        request.customPublisherConfigs = ListMultiFrameCodec.decode(iterator, WanCustomPublisherConfigHolderCodec::decode);
        request.batchPublisherConfigs = ListMultiFrameCodec.decode(iterator, WanBatchPublisherConfigHolderCodec::decode);
        return request;
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }
}

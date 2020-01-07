/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * Adds a new reliable topic configuration to a running cluster.
 * If a reliable topic configuration with the given {@code name} already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
@Generated("095351ea8fccce605aab0938cfb042c3")
public final class DynamicConfigAddReliableTopicConfigCodec {
    //hex: 0x1B0D00
    public static final int REQUEST_MESSAGE_TYPE = 1772800;
    //hex: 0x1B0D01
    public static final int RESPONSE_MESSAGE_TYPE = 1772801;
    private static final int REQUEST_READ_BATCH_SIZE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_STATISTICS_ENABLED_FIELD_OFFSET = REQUEST_READ_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private DynamicConfigAddReliableTopicConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of reliable topic
         */
        public java.lang.String name;

        /**
         * message listener configurations
         */
        public @Nullable java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs;

        /**
         * maximum number of items to read in a batch.
         */
        public int readBatchSize;

        /**
         * {@code true} to enable gathering of statistics, otherwise {@code false}
         */
        public boolean statisticsEnabled;

        /**
         * policy to handle an overloaded topic. Available values are {@code DISCARD_OLDEST},
         * {@code DISCARD_NEWEST}, {@code BLOCK} and {@code ERROR}.
         */
        public java.lang.String topicOverloadPolicy;

        /**
         * a serialized {@link java.util.concurrent.Executor} instance to use for executing
         * message listeners or {@code null}
         */
        public @Nullable com.hazelcast.internal.serialization.Data executor;
    }

    public static ClientMessage encodeRequest(java.lang.String name, @Nullable java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs, int readBatchSize, boolean statisticsEnabled, java.lang.String topicOverloadPolicy, @Nullable com.hazelcast.internal.serialization.Data executor) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("DynamicConfig.AddReliableTopicConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_READ_BATCH_SIZE_FIELD_OFFSET, readBatchSize);
        encodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET, statisticsEnabled);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        ListMultiFrameCodec.encodeNullable(clientMessage, listenerConfigs, ListenerConfigHolderCodec::encode);
        StringCodec.encode(clientMessage, topicOverloadPolicy);
        CodecUtil.encodeNullable(clientMessage, executor, DataCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddReliableTopicConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.readBatchSize = decodeInt(initialFrame.content, REQUEST_READ_BATCH_SIZE_FIELD_OFFSET);
        request.statisticsEnabled = decodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.listenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        request.topicOverloadPolicy = StringCodec.decode(iterator);
        request.executor = CodecUtil.decodeNullable(iterator, DataCodec::decode);
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

    public static DynamicConfigAddReliableTopicConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}

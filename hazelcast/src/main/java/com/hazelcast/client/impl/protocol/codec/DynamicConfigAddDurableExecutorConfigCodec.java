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
 * Adds a new durable executor configuration to a running cluster.
 * If a durable executor configuration with the given {@code name} already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
@Generated("ab44138a9a02e22bfc22da925227b55b")
public final class DynamicConfigAddDurableExecutorConfigCodec {
    //hex: 0x1B0900
    public static final int REQUEST_MESSAGE_TYPE = 1771776;
    //hex: 0x1B0901
    public static final int RESPONSE_MESSAGE_TYPE = 1771777;
    private static final int REQUEST_POOL_SIZE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_DURABILITY_FIELD_OFFSET = REQUEST_POOL_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_CAPACITY_FIELD_OFFSET = REQUEST_DURABILITY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_STATISTICS_ENABLED_FIELD_OFFSET = REQUEST_CAPACITY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private DynamicConfigAddDurableExecutorConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * durable executor name
         */
        public java.lang.String name;

        /**
         * executor thread pool size
         */
        public int poolSize;

        /**
         * executor's durability
         */
        public int durability;

        /**
         * capacity of executor tasks per partition
         */
        public int capacity;

        /**
         * name of an existing configured split brain protection to be used to determine the minimum number of members
         * required in the cluster for the lock to remain functional. When {@code null}, split brain protection does not
         * apply to this lock configuration's operations.
         */
        public @Nullable java.lang.String splitBrainProtectionName;

        /**
         * {@code true} to enable gathering of statistics, otherwise {@code false}
         */
        public boolean statisticsEnabled;

        /**
         * True if the statisticsEnabled is received from the client, false otherwise.
         * If this is false, statisticsEnabled has the default value for its type.
         */
        public boolean isStatisticsEnabledExists;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int poolSize, int durability, int capacity, @Nullable java.lang.String splitBrainProtectionName, boolean statisticsEnabled) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("DynamicConfig.AddDurableExecutorConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_POOL_SIZE_FIELD_OFFSET, poolSize);
        encodeInt(initialFrame.content, REQUEST_DURABILITY_FIELD_OFFSET, durability);
        encodeInt(initialFrame.content, REQUEST_CAPACITY_FIELD_OFFSET, capacity);
        encodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET, statisticsEnabled);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        CodecUtil.encodeNullable(clientMessage, splitBrainProtectionName, StringCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddDurableExecutorConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.poolSize = decodeInt(initialFrame.content, REQUEST_POOL_SIZE_FIELD_OFFSET);
        request.durability = decodeInt(initialFrame.content, REQUEST_DURABILITY_FIELD_OFFSET);
        request.capacity = decodeInt(initialFrame.content, REQUEST_CAPACITY_FIELD_OFFSET);
        if (initialFrame.content.length >= REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            request.statisticsEnabled = decodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET);
            request.isStatisticsEnabledExists = true;
        } else {
            request.isStatisticsEnabledExists = false;
        }
        request.name = StringCodec.decode(iterator);
        request.splitBrainProtectionName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
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

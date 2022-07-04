/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("8a5e7ff16ef373bfed1830e63dac06e7")
public final class QueryCacheConfigHolderCodec {
    private static final int BATCH_SIZE_FIELD_OFFSET = 0;
    private static final int BUFFER_SIZE_FIELD_OFFSET = BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int DELAY_SECONDS_FIELD_OFFSET = BUFFER_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INCLUDE_VALUE_FIELD_OFFSET = DELAY_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int POPULATE_FIELD_OFFSET = INCLUDE_VALUE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int COALESCE_FIELD_OFFSET = POPULATE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int SERIALIZE_KEYS_FIELD_OFFSET = COALESCE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = SERIALIZE_KEYS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private QueryCacheConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder queryCacheConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, BATCH_SIZE_FIELD_OFFSET, queryCacheConfigHolder.getBatchSize());
        encodeInt(initialFrame.content, BUFFER_SIZE_FIELD_OFFSET, queryCacheConfigHolder.getBufferSize());
        encodeInt(initialFrame.content, DELAY_SECONDS_FIELD_OFFSET, queryCacheConfigHolder.getDelaySeconds());
        encodeBoolean(initialFrame.content, INCLUDE_VALUE_FIELD_OFFSET, queryCacheConfigHolder.isIncludeValue());
        encodeBoolean(initialFrame.content, POPULATE_FIELD_OFFSET, queryCacheConfigHolder.isPopulate());
        encodeBoolean(initialFrame.content, COALESCE_FIELD_OFFSET, queryCacheConfigHolder.isCoalesce());
        encodeBoolean(initialFrame.content, SERIALIZE_KEYS_FIELD_OFFSET, queryCacheConfigHolder.isSerializeKeys());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, queryCacheConfigHolder.getInMemoryFormat());
        StringCodec.encode(clientMessage, queryCacheConfigHolder.getName());
        PredicateConfigHolderCodec.encode(clientMessage, queryCacheConfigHolder.getPredicateConfigHolder());
        EvictionConfigHolderCodec.encode(clientMessage, queryCacheConfigHolder.getEvictionConfigHolder());
        ListMultiFrameCodec.encodeNullable(clientMessage, queryCacheConfigHolder.getListenerConfigs(), ListenerConfigHolderCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, queryCacheConfigHolder.getIndexConfigs(), IndexConfigCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int batchSize = decodeInt(initialFrame.content, BATCH_SIZE_FIELD_OFFSET);
        int bufferSize = decodeInt(initialFrame.content, BUFFER_SIZE_FIELD_OFFSET);
        int delaySeconds = decodeInt(initialFrame.content, DELAY_SECONDS_FIELD_OFFSET);
        boolean includeValue = decodeBoolean(initialFrame.content, INCLUDE_VALUE_FIELD_OFFSET);
        boolean populate = decodeBoolean(initialFrame.content, POPULATE_FIELD_OFFSET);
        boolean coalesce = decodeBoolean(initialFrame.content, COALESCE_FIELD_OFFSET);
        boolean isSerializeKeysExists = false;
        boolean serializeKeys = false;
        if (initialFrame.content.length >= SERIALIZE_KEYS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            serializeKeys = decodeBoolean(initialFrame.content, SERIALIZE_KEYS_FIELD_OFFSET);
            isSerializeKeysExists = true;
        }

        java.lang.String inMemoryFormat = StringCodec.decode(iterator);
        java.lang.String name = StringCodec.decode(iterator);
        com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder predicateConfigHolder = PredicateConfigHolderCodec.decode(iterator);
        com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfigHolder = EvictionConfigHolderCodec.decode(iterator);
        java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        java.util.List<com.hazelcast.config.IndexConfig> indexConfigs = ListMultiFrameCodec.decodeNullable(iterator, IndexConfigCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder(batchSize, bufferSize, delaySeconds, includeValue, populate, coalesce, inMemoryFormat, name, predicateConfigHolder, evictionConfigHolder, listenerConfigs, indexConfigs, isSerializeKeysExists, serializeKeys);
    }
}

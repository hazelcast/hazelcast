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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.nio.Bits;

import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class QueryCacheConfigHolderCodec {
    private static final int BATCH_SIZE_OFFSET = 0;
    private static final int BUFFER_SIZE_OFFSET = BATCH_SIZE_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int DELAY_SECONDS_OFFSET = BUFFER_SIZE_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int INCLUDE_VALUE_OFFSET = DELAY_SECONDS_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int POPULATE_OFFSET = INCLUDE_VALUE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int COALESCE_OFFSET = POPULATE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = COALESCE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private QueryCacheConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, QueryCacheConfigHolder configHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, BATCH_SIZE_OFFSET, configHolder.getBatchSize());
        encodeInt(initialFrame.content, BUFFER_SIZE_OFFSET, configHolder.getBufferSize());
        encodeInt(initialFrame.content, DELAY_SECONDS_OFFSET, configHolder.getDelaySeconds());
        encodeBoolean(initialFrame.content, INCLUDE_VALUE_OFFSET, configHolder.isIncludeValue());
        encodeBoolean(initialFrame.content, POPULATE_OFFSET, configHolder.isPopulate());
        encodeBoolean(initialFrame.content, COALESCE_OFFSET, configHolder.isCoalesce());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, configHolder.getInMemoryFormat());
        StringCodec.encode(clientMessage, configHolder.getName());
        PredicateConfigHolderCodec.encode(clientMessage, configHolder.getPredicateConfigHolder());
        EvictionConfigHolderCodec.encode(clientMessage, configHolder.getEvictionConfigHolder());
        ListMultiFrameCodec.encodeNullable(clientMessage, configHolder.getListenerConfigs(), ListenerConfigHolderCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, configHolder.getIndexConfigs(), MapIndexConfigCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static QueryCacheConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int batchSize = decodeInt(initialFrame.content, BATCH_SIZE_OFFSET);
        int bufferSize = decodeInt(initialFrame.content, BUFFER_SIZE_OFFSET);
        int delaySeconds = decodeInt(initialFrame.content, DELAY_SECONDS_OFFSET);
        boolean includeValue = decodeBoolean(initialFrame.content, INCLUDE_VALUE_OFFSET);
        boolean populate = decodeBoolean(initialFrame.content, POPULATE_OFFSET);
        boolean coalesce = decodeBoolean(initialFrame.content, COALESCE_OFFSET);

        String inMemoryFormat = StringCodec.decode(iterator);
        String name = StringCodec.decode(iterator);
        PredicateConfigHolder predicateConfigHolder = PredicateConfigHolderCodec.decode(iterator);
        EvictionConfigHolder evictionConfigHolder = EvictionConfigHolderCodec.decode(iterator);
        List<ListenerConfigHolder> listenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        List<MapIndexConfig> indexConfigs = ListMultiFrameCodec.decodeNullable(iterator, MapIndexConfigCodec::decode);

        fastForwardToEndFrame(iterator);

        QueryCacheConfigHolder queryCacheConfigHolder = new QueryCacheConfigHolder();
        queryCacheConfigHolder.setBatchSize(batchSize);
        queryCacheConfigHolder.setBufferSize(bufferSize);
        queryCacheConfigHolder.setDelaySeconds(delaySeconds);
        queryCacheConfigHolder.setIncludeValue(includeValue);
        queryCacheConfigHolder.setPopulate(populate);
        queryCacheConfigHolder.setCoalesce(coalesce);
        queryCacheConfigHolder.setInMemoryFormat(inMemoryFormat);
        queryCacheConfigHolder.setName(name);
        queryCacheConfigHolder.setPredicateConfigHolder(predicateConfigHolder);
        queryCacheConfigHolder.setEvictionConfigHolder(evictionConfigHolder);
        queryCacheConfigHolder.setListenerConfigs(listenerConfigs);
        queryCacheConfigHolder.setIndexConfigs(indexConfigs);
        return queryCacheConfigHolder;
    }
}

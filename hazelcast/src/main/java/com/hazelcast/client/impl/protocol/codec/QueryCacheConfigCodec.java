/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.annotation.Codec;
import com.hazelcast.annotation.Since;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.nio.Bits;

import java.util.ArrayList;
import java.util.List;

@Codec(QueryCacheConfigHolder.class)
@Since("1.5")
public final class QueryCacheConfigCodec {

    private static final int ENCODED_BOOLEANS = 5;
    private static final int ENCODED_INTS = 3;

    private QueryCacheConfigCodec() {
    }

    public static QueryCacheConfigHolder decode(ClientMessage clientMessage) {
        QueryCacheConfigHolder config = new QueryCacheConfigHolder();
        config.setBatchSize(clientMessage.getInt());
        config.setBufferSize(clientMessage.getInt());
        config.setDelaySeconds(clientMessage.getInt());
        config.setIncludeValue(clientMessage.getBoolean());
        config.setPopulate(clientMessage.getBoolean());
        config.setCoalesce(clientMessage.getBoolean());
        config.setInMemoryFormat(clientMessage.getStringUtf8());
        config.setName(clientMessage.getStringUtf8());
        config.setPredicateConfigHolder(PredicateConfigCodec.decode(clientMessage));
        config.setEvictionConfigHolder(EvictionConfigCodec.decode(clientMessage));
        boolean isNullListenerConfigs = clientMessage.getBoolean();
        List<ListenerConfigHolder> listenerConfigHolders = null;
        if (!isNullListenerConfigs) {
            int listenersCount = clientMessage.getInt();
            listenerConfigHolders = new ArrayList<ListenerConfigHolder>(listenersCount);
            for (int i = 0; i < listenersCount; i++) {
                 listenerConfigHolders.add(ListenerConfigCodec.decode(clientMessage));
            }
        }
        config.setListenerConfigs(listenerConfigHolders);
        boolean isNullIndexConfigs = clientMessage.getBoolean();
        List<MapIndexConfig> indexConfigs = null;
        if (!isNullIndexConfigs) {
            int indexConfigCount = clientMessage.getInt();
            indexConfigs = new ArrayList<MapIndexConfig>(indexConfigCount);
            for (int i = 0; i < indexConfigCount; i++) {
                indexConfigs.add(MapIndexConfigCodec.decode(clientMessage));
            }
        }
        config.setIndexConfigs(indexConfigs);
        return config;
    }

    public static void encode(QueryCacheConfigHolder config, ClientMessage clientMessage) {
        clientMessage.set(config.getBatchSize())
                     .set(config.getBufferSize())
                     .set(config.getDelaySeconds())
                     .set(config.isIncludeValue())
                     .set(config.isPopulate())
                     .set(config.isCoalesce())
                     .set(config.getInMemoryFormat())
                     .set(config.getName());
        PredicateConfigCodec.encode(config.getPredicateConfigHolder(), clientMessage);
        EvictionConfigCodec.encode(config.getEvictionConfigHolder(), clientMessage);
        boolean isNullListenerConfigs = config.getListenerConfigs() == null;
        clientMessage.set(isNullListenerConfigs);
        if (!isNullListenerConfigs) {
            clientMessage.set(config.getListenerConfigs().size());
            for (ListenerConfigHolder listenerConfigHolder : config.getListenerConfigs()) {
                ListenerConfigCodec.encode(listenerConfigHolder, clientMessage);
            }
        }
        boolean isNullIndexConfigs = config.getIndexConfigs() == null;
        clientMessage.set(isNullIndexConfigs);
        if (!isNullIndexConfigs) {
            clientMessage.set(config.getIndexConfigs().size());
            for (MapIndexConfig indexConfig : config.getIndexConfigs()) {
                MapIndexConfigCodec.encode(indexConfig, clientMessage);
            }
        }
    }

    public static int calculateDataSize(QueryCacheConfigHolder config) {
        int dataSize = ENCODED_INTS * Bits.INT_SIZE_IN_BYTES + ENCODED_BOOLEANS * Bits.BOOLEAN_SIZE_IN_BYTES;
        if (config.getIndexConfigs() != null && !config.getIndexConfigs().isEmpty()) {
            dataSize += Bits.INT_SIZE_IN_BYTES;
            for (MapIndexConfig indexConfig : config.getIndexConfigs()) {
                dataSize += MapIndexConfigCodec.calculateDataSize(indexConfig);
            }
        }
        if (config.getListenerConfigs() != null && !config.getListenerConfigs().isEmpty()) {
            dataSize += Bits.INT_SIZE_IN_BYTES;
            for (ListenerConfigHolder listenerConfig : config.getListenerConfigs()) {
                dataSize += ListenerConfigCodec.calculateDataSize(listenerConfig);
            }
        }
        dataSize += ParameterUtil.calculateDataSize(config.getName());
        dataSize += ParameterUtil.calculateDataSize(config.getInMemoryFormat());
        dataSize += PredicateConfigCodec.calculateDataSize(config.getPredicateConfigHolder());
        dataSize += EvictionConfigCodec.calculateDataSize(config.getEvictionConfigHolder());
        return dataSize;
    }
}

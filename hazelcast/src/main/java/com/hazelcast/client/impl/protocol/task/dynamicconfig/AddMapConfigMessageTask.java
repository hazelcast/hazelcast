/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.Node;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.cluster.Versions.V3_10;

public class AddMapConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddMapConfigCodec.RequestParameters> {

    public AddMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddMapConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddMapConfigCodec.encodeResponse();
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    protected IdentifiedDataSerializable getConfig() {
        MapConfig config = new MapConfig(parameters.name);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setBackupCount(parameters.backupCount);
        config.setCacheDeserializedValues(CacheDeserializedValues.valueOf(parameters.cacheDeserializedValues));
        config.setEvictionPolicy(EvictionPolicy.valueOf(parameters.evictionPolicy));
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            config.setEntryListenerConfigs(
                    (List<EntryListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs));
        }
        config.setHotRestartConfig(parameters.hotRestartConfig);
        config.setInMemoryFormat(InMemoryFormat.valueOf(parameters.inMemoryFormat));
        config.setMapAttributeConfigs(parameters.mapAttributeConfigs);
        if (parameters.mapEvictionPolicy != null) {
            MapEvictionPolicy evictionPolicy = serializationService.toObject(parameters.mapEvictionPolicy);
            config.setMapEvictionPolicy(evictionPolicy);
        }
        config.setMapIndexConfigs(parameters.mapIndexConfigs);
        if (parameters.mapStoreConfig != null) {
            config.setMapStoreConfig(parameters.mapStoreConfig.asMapStoreConfig(serializationService));
        }
        config.setTimeToLiveSeconds(parameters.timeToLiveSeconds);
        config.setMaxIdleSeconds(parameters.maxIdleSeconds);
        config.setMaxSizeConfig(new MaxSizeConfig(parameters.maxSizeConfigSize,
                MaxSizeConfig.MaxSizePolicy.valueOf(parameters.maxSizeConfigMaxSizePolicy)));
        Version clusterVersion = nodeEngine.getClusterService().getClusterVersion();
        if (clusterVersion.isGreaterOrEqual(V3_10) && parameters.mergeBatchSizeExist) {
            MergePolicyConfig mergePolicyConfig = mergePolicyConfig(true, parameters.mergePolicy,
                    parameters.mergeBatchSize);
            config.setMergePolicyConfig(mergePolicyConfig);
        } else {
            // RU_COMPAT_3_9
            config.setMergePolicy(parameters.mergePolicy);
        }
        if (parameters.nearCacheConfig != null) {
            config.setNearCacheConfig(parameters.nearCacheConfig.asNearCacheConfig(serializationService));
        }
        config.setPartitioningStrategyConfig(getPartitioningStrategyConfig());
        if (parameters.partitionLostListenerConfigs != null && !parameters.partitionLostListenerConfigs.isEmpty()) {
            config.setPartitionLostListenerConfigs(
                    (List<MapPartitionLostListenerConfig>) adaptListenerConfigs(parameters.partitionLostListenerConfigs));
        }
        config.setQuorumName(parameters.quorumName);
        if (parameters.queryCacheConfigs != null && !parameters.queryCacheConfigs.isEmpty()) {
            List<QueryCacheConfig> queryCacheConfigs = new ArrayList<QueryCacheConfig>(parameters.queryCacheConfigs.size());
            for (QueryCacheConfigHolder holder : parameters.queryCacheConfigs) {
                queryCacheConfigs.add(holder.asQueryCacheConfig(serializationService));
            }
            config.setQueryCacheConfigs(queryCacheConfigs);
        }
        config.setWanReplicationRef(parameters.wanReplicationRef);
        return config;
    }

    private PartitioningStrategyConfig getPartitioningStrategyConfig() {
        if (parameters.partitioningStrategyClassName != null) {
            return new PartitioningStrategyConfig(parameters.partitioningStrategyClassName);
        } else if (parameters.partitioningStrategyImplementation != null) {
            PartitioningStrategy partitioningStrategy =
                    serializationService.toObject(parameters.partitioningStrategyImplementation);
            return new PartitioningStrategyConfig(partitioningStrategy);
        } else {
            return null;
        }
    }

    @Override
    public String getMethodName() {
        return "addMapConfig";
    }
}

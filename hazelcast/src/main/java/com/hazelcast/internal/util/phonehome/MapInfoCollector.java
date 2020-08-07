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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.DistributedObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static java.util.stream.Collectors.toList;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;

class MapInfoCollector implements MetricsCollector {

    private static final int DISTRIBUTED_OBJECT_TYPE_COUNT = 13;
    private static final Predicate<MapConfig> IS_MAP_STORE_ENABLED = mapConfig -> mapConfig.getMapStoreConfig().isEnabled();
    private Collection<MapConfig> mapConfigs;

    @Override
    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();
        mapConfigs = distributedObjects.stream()
                .filter(distributedObject -> distributedObject.getServiceName().equals(MapService.SERVICE_NAME))
                .map(distributedObject -> hazelcastNode.getConfig().getMapConfig(distributedObject.getName()))
                .filter(Objects::nonNull)
                .collect(toList());

        Map<PhoneHomeMetrics, String> mapInfo = new HashMap<>(DISTRIBUTED_OBJECT_TYPE_COUNT);

        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED, String.valueOf(countMapWithBackupReadEnabled()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED, String.valueOf(countMapWithMapStoreEnabled()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE, String.valueOf(countMapWithAtleastOneQueryCache()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX, String.valueOf(countMapWithAtleastOneIndex()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_ENABLED, String.valueOf(countMapWithHotRestartEnabled()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION, String.valueOf(countMapWithWANReplication()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE, String.valueOf(countMapWithAtleastOneAttribute()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION, String.valueOf(countMapUsingEviction()));
        mapInfo.put(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT, String.valueOf(countMapWithNativeInMemoryFormat()));
        mapInfo.put(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE,
                String.valueOf(mapOperationLatency(hazelcastNode, IS_MAP_STORE_ENABLED,
                        LocalMapStats::getTotalPutLatency, LocalMapStats::getPutOperationCount)));
        mapInfo.put(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE,
                String.valueOf(mapOperationLatency(hazelcastNode, IS_MAP_STORE_ENABLED.negate(),
                        LocalMapStats::getTotalPutLatency, LocalMapStats::getPutOperationCount)));
        mapInfo.put(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE,
                String.valueOf(mapOperationLatency(hazelcastNode, IS_MAP_STORE_ENABLED,
                        LocalMapStats::getTotalGetLatency, LocalMapStats::getGetOperationCount)));
        mapInfo.put(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE,
                String.valueOf(mapOperationLatency(hazelcastNode, IS_MAP_STORE_ENABLED.negate(),
                        LocalMapStats::getTotalGetLatency, LocalMapStats::getGetOperationCount)));

        return mapInfo;
    }

    private long countMapWithBackupReadEnabled() {
        return mapConfigs.stream()
                .filter(MapConfig::isReadBackupData).count();
    }

    private long countMapWithMapStoreEnabled() {
        return mapConfigs.stream().map(MapConfig::getMapStoreConfig)
                .filter(MapStoreConfig::isEnabled).count();
    }

    private long countMapWithAtleastOneQueryCache() {
        return mapConfigs.stream().map(MapConfig::getQueryCacheConfigs)
                .filter(queryCacheConfigs -> !(queryCacheConfigs.isEmpty())).count();
    }

    private long countMapWithAtleastOneIndex() {
        return mapConfigs.stream().map(MapConfig::getIndexConfigs)
                .filter(indexConfigs -> !(indexConfigs.isEmpty())).count();
    }

    private long countMapWithHotRestartEnabled() {
        return mapConfigs.stream().map(MapConfig::getHotRestartConfig)
                .filter(HotRestartConfig::isEnabled).count();
    }

    private long countMapWithWANReplication() {
        return mapConfigs.stream().map(MapConfig::getWanReplicationRef)
                .filter(Objects::nonNull).count();
    }

    private long countMapWithAtleastOneAttribute() {
        return mapConfigs.stream().map(MapConfig::getAttributeConfigs)
                .filter(attributeConfigs -> !(attributeConfigs.isEmpty())).count();
    }

    private long countMapUsingEviction() {
        return mapConfigs.stream().map(MapConfig::getEvictionConfig)
                .filter(evictionConfig -> evictionConfig.getEvictionPolicy() != EvictionPolicy.NONE).count();
    }

    private long countMapWithNativeInMemoryFormat() {
        return mapConfigs.stream().map(MapConfig::getInMemoryFormat)
                .filter(inMemoryFormat -> inMemoryFormat == InMemoryFormat.NATIVE).count();
    }

    static class LatencyInfo {
        private long totalLatency;
        private long operationCount;

        LatencyInfo(long totalLatency, long operationCount) {
            this.totalLatency = totalLatency;
            this.operationCount = operationCount;
        }

        void add(long totalLatency, long operationCount) {
            this.totalLatency += totalLatency;
            this.operationCount += operationCount;
        }

        long calculateAverage() {
            return this.operationCount == 0 ? -1 : (this.totalLatency / this.operationCount);
        }
    }

    private long mapOperationLatency(Node node, Predicate<MapConfig> predicate,
                                     ToLongFunction<LocalMapStats> totalLatencyProvider,
                                     ToLongFunction<LocalMapStats> operationCountProvider) {

        LatencyInfo latencyInfo = new LatencyInfo(0L, 0L);
        mapConfigs.stream().filter(predicate)
                .map(mapConfig -> node.hazelcastInstance.getMap(mapConfig.getName()).getLocalMapStats())
                .forEach(mapStats -> latencyInfo.add(totalLatencyProvider.applyAsLong(mapStats),
                        operationCountProvider.applyAsLong(mapStats)));
        return latencyInfo.calculateAverage();
    }
}

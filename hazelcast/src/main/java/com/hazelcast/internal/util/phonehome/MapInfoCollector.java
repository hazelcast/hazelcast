/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static java.util.stream.Collectors.toList;

/**
 * Collects information about IMap
 */
class MapInfoCollector implements MetricsCollector {

    private static final Predicate<MapConfig> IS_MAP_STORE_ENABLED = mapConfig -> mapConfig.getMapStoreConfig().isEnabled();
    private Collection<MapConfig> mapConfigs;

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        Collection<DistributedObject> objects = node.hazelcastInstance.getDistributedObjects();
        mapConfigs = objects.stream()
                            .filter(obj -> obj.getServiceName().equals(MapService.SERVICE_NAME))
                            .map(obj -> node.getConfig().getMapConfig(obj.getName()))
                            .filter(Objects::nonNull)
                            .collect(toList());
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED,
                String.valueOf(countMapWithBackupReadEnabled()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED,
                String.valueOf(countMapWithMapStoreEnabled()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE,
                String.valueOf(countMapWithAtleastOneQueryCache()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX,
                String.valueOf(countMapWithAtleastOneIndex()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_ENABLED,
                String.valueOf(countMapWithHotRestartEnabled()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION,
                String.valueOf(countMapWithWANReplication()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE,
                String.valueOf(countMapWithAtleastOneAttribute()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_USING_EVICTION,
                String.valueOf(countMapUsingEviction()));
        metricsConsumer.accept(PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT,
                String.valueOf(countMapWithNativeInMemoryFormat()));
        metricsConsumer.accept(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE,
                String.valueOf(mapOperationLatency(node, IS_MAP_STORE_ENABLED,
                        LocalMapStats::getTotalPutLatency, LocalMapStats::getPutOperationCount)));
        metricsConsumer.accept(PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE,
                String.valueOf(mapOperationLatency(node, IS_MAP_STORE_ENABLED.negate(),
                        LocalMapStats::getTotalPutLatency, LocalMapStats::getPutOperationCount)));
        metricsConsumer.accept(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE,
                String.valueOf(mapOperationLatency(node, IS_MAP_STORE_ENABLED,
                        LocalMapStats::getTotalGetLatency, LocalMapStats::getGetOperationCount)));
        metricsConsumer.accept(PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE,
                String.valueOf(mapOperationLatency(node, IS_MAP_STORE_ENABLED.negate(),
                        LocalMapStats::getTotalGetLatency, LocalMapStats::getGetOperationCount)));
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

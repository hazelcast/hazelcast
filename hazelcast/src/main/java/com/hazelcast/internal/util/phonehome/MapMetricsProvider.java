/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_USING_EVICTION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_INDEX;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_MAP_STORE_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_READ_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MAP_COUNT_WITH_WAN_REPLICATION;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_MAP_ENTRYSET_CALLS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_MAP_QUERY_SIZE_LIMITER_HITS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TOTAL_MAP_VALUES_CALLS;

/**
 * Collects information about IMap
 */
class MapMetricsProvider implements MetricsProvider {
    private static final Predicate<MapConfig> IS_MAP_STORE_ENABLED = mapConfig -> mapConfig.getMapStoreConfig().isEnabled();

    private Map<String, MapConfig> mapConfigs;

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        initMapConfigs(node);
        context.collect(MAP_COUNT_WITH_READ_ENABLED, countMapWithBackupReadEnabled());
        context.collect(MAP_COUNT_WITH_MAP_STORE_ENABLED, countMapWithMapStoreEnabled());
        context.collect(MAP_COUNT_WITH_ATLEAST_ONE_QUERY_CACHE, countMapWithAtleastOneQueryCache());
        context.collect(MAP_COUNT_WITH_ATLEAST_ONE_INDEX, countMapWithAtleastOneIndex());
        context.collect(MAP_COUNT_WITH_HOT_RESTART_OR_PERSISTENCE_ENABLED, countMapWithHotRestartOrPersistenceEnabled());
        context.collect(MAP_COUNT_WITH_WAN_REPLICATION, countMapWithWANReplication());
        context.collect(MAP_COUNT_WITH_ATLEAST_ONE_ATTRIBUTE, countMapWithAtleastOneAttribute());
        context.collect(MAP_COUNT_USING_EVICTION, countMapUsingEviction());
        context.collect(MAP_COUNT_USING_NATIVE_INMEMORY_FORMAT, countMapWithNativeInMemoryFormat());
        context.collect(AVERAGE_PUT_LATENCY_OF_MAPS_USING_MAPSTORE,
                mapOperationLatency(node, IS_MAP_STORE_ENABLED,
                        LocalMapStats::getTotalPutLatency, LocalMapStats::getPutOperationCount));
        context.collect(AVERAGE_PUT_LATENCY_OF_MAPS_WITHOUT_MAPSTORE,
                mapOperationLatency(node, IS_MAP_STORE_ENABLED.negate(),
                        LocalMapStats::getTotalPutLatency, LocalMapStats::getPutOperationCount));
        context.collect(AVERAGE_GET_LATENCY_OF_MAPS_USING_MAPSTORE,
                mapOperationLatency(node, IS_MAP_STORE_ENABLED,
                        LocalMapStats::getTotalGetLatency, LocalMapStats::getGetOperationCount));
        context.collect(AVERAGE_GET_LATENCY_OF_MAPS_WITHOUT_MAPSTORE,
                mapOperationLatency(node, IS_MAP_STORE_ENABLED.negate(),
                        LocalMapStats::getTotalGetLatency, LocalMapStats::getGetOperationCount));
        context.collect(TOTAL_MAP_VALUES_CALLS, mapCallAggregator(node, LocalMapStats::getValuesCallCount));
        context.collect(TOTAL_MAP_ENTRYSET_CALLS, mapCallAggregator(node, LocalMapStats::getEntrySetCallCount));
        context.collect(TOTAL_MAP_QUERY_SIZE_LIMITER_HITS,
                mapCallAggregator(node, LocalMapStats::getQueryResultSizeExceededCount));
    }

    private void initMapConfigs(Node node) {
        Collection<DistributedObject> objects = node.hazelcastInstance.getDistributedObjects();
        mapConfigs = new HashMap<>();
        objects.stream()
                .filter(obj -> obj.getServiceName().equals(MapService.SERVICE_NAME))
                .forEach(obj -> {
                    MapConfig config = node.getConfig().findMapConfig(obj.getName());
                    if (config != null) {
                        mapConfigs.put(obj.getName(), config);
                    }
                });
    }

    private long countMapWithBackupReadEnabled() {
        return mapConfigs.values().stream()
                .filter(MapConfig::isReadBackupData).count();
    }

    private long countMapWithMapStoreEnabled() {
        return mapConfigs.values().stream().map(MapConfig::getMapStoreConfig)
                .filter(MapStoreConfig::isEnabled).count();
    }

    private long countMapWithAtleastOneQueryCache() {
        return mapConfigs.values().stream().map(MapConfig::getQueryCacheConfigs)
                .filter(queryCacheConfigs -> !(queryCacheConfigs.isEmpty())).count();
    }

    private long countMapWithAtleastOneIndex() {
        return mapConfigs.values().stream().map(MapConfig::getIndexConfigs)
                .filter(indexConfigs -> !(indexConfigs.isEmpty())).count();
    }

    private long countMapWithHotRestartOrPersistenceEnabled() {
        return mapConfigs.values().stream()
                .filter(mapConfig ->
                        mapConfig.getDataPersistenceConfig().isEnabled() || mapConfig.getHotRestartConfig().isEnabled())
                .count();
    }

    private long countMapWithWANReplication() {
        return mapConfigs.values().stream().map(MapConfig::getWanReplicationRef)
                .filter(Objects::nonNull).count();
    }

    private long countMapWithAtleastOneAttribute() {
        return mapConfigs.values().stream().map(MapConfig::getAttributeConfigs)
                .filter(attributeConfigs -> !(attributeConfigs.isEmpty())).count();
    }

    private long countMapUsingEviction() {
        return mapConfigs.values().stream().map(MapConfig::getEvictionConfig)
                .filter(evictionConfig -> evictionConfig.getEvictionPolicy() != EvictionPolicy.NONE).count();
    }

    private long countMapWithNativeInMemoryFormat() {
        return mapConfigs.values().stream().map(MapConfig::getInMemoryFormat)
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
        mapConfigs.entrySet().stream()
                .filter(entry -> predicate.test(entry.getValue()))
                .map(entry -> node.hazelcastInstance.getMap(entry.getKey()).getLocalMapStats())
                .forEach(mapStats -> latencyInfo.add(totalLatencyProvider.applyAsLong(mapStats),
                        operationCountProvider.applyAsLong(mapStats)));
        return latencyInfo.calculateAverage();
    }

    private long mapCallAggregator(Node node,
                                   ToLongFunction<LocalMapStats> mapStatProvider) {
        return mapConfigs.keySet().stream()
                .map(mapConfig -> node.hazelcastInstance.getMap(mapConfig).getLocalMapStats())
                .mapToLong(mapStatProvider::applyAsLong)
                .sum();
    }
}

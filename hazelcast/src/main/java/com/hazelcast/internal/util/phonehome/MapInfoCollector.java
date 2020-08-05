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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;

class MapInfoCollector implements MetricsCollector {

    private static final int DISTRIBUTED_OBJECT_TYPE_COUNT = 13;
    private Collection<MapConfig> mapConfigs;

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        mapConfigs = distributedObjects.stream()
                .filter(distributedObject -> distributedObject.getServiceName().equals(MapService.SERVICE_NAME))
                .map(distributedObject -> hazelcastNode.getConfig().getMapConfig(distributedObject.getName()))
                .filter(Objects::nonNull)
                .collect(toList());

        Map<String, String> mapInfo = new HashMap<>(DISTRIBUTED_OBJECT_TYPE_COUNT);

        mapInfo.put("mpbrct", String.valueOf(countMapWithBackupReadEnabled()));
        mapInfo.put("mpmsct", String.valueOf(countMapWithMapStoreEnabled()));
        mapInfo.put("mpaoqcct", String.valueOf(countMapWithAtleastOneQueryCache()));
        mapInfo.put("mpaoict", String.valueOf(countMapWithAtleastOneIndex()));
        mapInfo.put("mphect", String.valueOf(countMapWithHotRestartEnabled()));
        mapInfo.put("mpwact", String.valueOf(countMapWithWANReplication()));
        mapInfo.put("mpaocct", String.valueOf(countMapWithAtleastOneAttribute()));
        mapInfo.put("mpevct", String.valueOf(countMapUsingEviction()));
        mapInfo.put("mpnmct", String.valueOf(countMapWithNativeInMemoryFormat()));
        mapInfo.put("mpptlams", String.valueOf(mapPutLatency(hazelcastNode, isMapStoreEnabled())));
        mapInfo.put("mpptla", String.valueOf(mapPutLatency(hazelcastNode, isMapStoreDisabled())));
        mapInfo.put("mpgtlams", String.valueOf(mapGetLatency(hazelcastNode, isMapStoreEnabled())));
        mapInfo.put("mpgtla", String.valueOf(mapGetLatency(hazelcastNode, isMapStoreDisabled())));

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

    static Predicate<MapConfig> isMapStoreEnabled() {
        return p -> p.getMapStoreConfig().isEnabled();
    }

    static Predicate<MapConfig> isMapStoreDisabled() {
        return p -> !p.getMapStoreConfig().isEnabled();
    }

    private static long calculateAverageLatency(List<LocalMapStats> mapStats, String operationType) {
        long[] latencyInfo = {0, 0};
        if (operationType.equals("put")) {
            mapStats.forEach(localMapStats -> {
                latencyInfo[0] += localMapStats.getTotalPutLatency();
                latencyInfo[1] += localMapStats.getPutOperationCount();
            });
        } else {
            mapStats.forEach(localMapStats -> {
                latencyInfo[0] += localMapStats.getTotalGetLatency();
                latencyInfo[1] += localMapStats.getGetOperationCount();
            });
        }
        return latencyInfo[1] == 0 ? -1 : (latencyInfo[0] / latencyInfo[1]);
    }

    private long mapPutLatency(Node node, Predicate<MapConfig> predicate) {

        List<LocalMapStats> mapStatsList = mapConfigs.stream()
                .filter(predicate)
                .map(mapConfig -> node.hazelcastInstance.getMap(mapConfig.getName()).getLocalMapStats())
                .filter(mapStats -> mapStats.getPutOperationCount() != 0L)
                .collect(toList());

        return calculateAverageLatency(mapStatsList, "put");
    }

    private long mapGetLatency(Node node, Predicate<MapConfig> predicate) {

        List<LocalMapStats> mapStatsList = mapConfigs.stream()
                .filter(predicate)
                .map(mapConfig -> node.hazelcastInstance.getMap(mapConfig.getName()).getLocalMapStats())
                .filter(mapStats -> mapStats.getGetOperationCount() != 0L)
                .collect(toList());

        return calculateAverageLatency(mapStatsList, "get");
    }
}

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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;

class MapInfoCollector implements MetricsCollector {

    private static final int COUNT_OF_MAP_METRICS = 9;
    Collection<MapConfig> mapConfigs;

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        mapConfigs = distributedObjects.stream()
                .filter(distributedObject -> distributedObject.getServiceName().equals(MapService.SERVICE_NAME))
                .map(distributedObject -> hazelcastNode.getConfig().getMapConfig(distributedObject.getName()))
                .collect(toList());

        Map<String, String> mapInfo = new HashMap<>(COUNT_OF_MAP_METRICS);

        mapInfo.put("mpbrct", String.valueOf(countMapWithBackupReadEnabled()));
        mapInfo.put("mpmsct", String.valueOf(countMapWithMapStoreEnabled()));
        mapInfo.put("mpaoqcct", String.valueOf(countMapWithAtleastOneQueryCache()));
        mapInfo.put("mpaoict", String.valueOf(countMapWithAtleastOneIndex()));
        mapInfo.put("mphect", String.valueOf(countMapWithHotRestartEnabled()));
        mapInfo.put("mpwact", String.valueOf(countMapWithWANReplication()));
        mapInfo.put("mpaocct", String.valueOf(countMapWithAtleastOneAttribute()));
        mapInfo.put("mpevct", String.valueOf(countMapUsingEviction()));
        mapInfo.put("mpnmct", String.valueOf(countMapWithNativeInMemoryFormat()));

        return mapInfo;
    }

    private long countMapWithBackupReadEnabled() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && mapConfig.isReadBackupData()).count();
    }

    private long countMapWithMapStoreEnabled() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && mapConfig.getMapStoreConfig().isEnabled()).count();
    }

    private long countMapWithAtleastOneQueryCache() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && !(mapConfig.getQueryCacheConfigs().isEmpty())).count();
    }

    private long countMapWithAtleastOneIndex() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && !(mapConfig.getIndexConfigs().isEmpty())).count();
    }

    private long countMapWithHotRestartEnabled() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && mapConfig.getHotRestartConfig().isEnabled()).count();
    }

    private long countMapWithWANReplication() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && mapConfig.getWanReplicationRef() != null).count();
    }

    private long countMapWithAtleastOneAttribute() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && !(mapConfig.getAttributeConfigs().isEmpty())).count();
    }

    private long countMapUsingEviction() {
        return mapConfigs.stream().filter(mapConfig -> mapConfig != null
                && mapConfig.getEvictionConfig().getEvictionPolicy() != EvictionPolicy.NONE).count();
    }

    private long countMapWithNativeInMemoryFormat() {
        return mapConfigs.stream()
                .filter(mapConfig -> mapConfig != null && mapConfig.getInMemoryFormat() == InMemoryFormat.NATIVE).count();
    }
}

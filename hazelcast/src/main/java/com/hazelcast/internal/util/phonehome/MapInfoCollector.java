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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;

class MapInfoCollector implements MetricsCollector {

    Collection<DistributedObject> maps;

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();
        maps = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(MapService.SERVICE_NAME)).collect(toList());
        Map<String, String> mapInfo = new HashMap<>();

        mapInfo.put("mpct", String.valueOf(maps.size()));
        mapInfo.put("mpbrct", String.valueOf(countMapWithBackupReadEnabled(hazelcastNode)));
        mapInfo.put("mpmsct", String.valueOf(countMapWithMapStoreEnabled(hazelcastNode)));
        mapInfo.put("mpaoqcct", String.valueOf(countMapWithAtleastOneQueryCache(hazelcastNode)));

        return mapInfo;
    }

    private long countMapWithBackupReadEnabled(Node node) {
        return maps.stream().filter(distributedObject -> {
            MapConfig config = node.getConfig().getMapConfig(distributedObject.getName());
            if (config != null) {
                return config.isReadBackupData();
            }
            return false;
        }).count();
    }

    private long countMapWithMapStoreEnabled(Node node) {
        return maps.stream().filter(distributedObject -> {
            MapConfig config = node.getConfig().getMapConfig(distributedObject.getName());
            if (config != null) {
                return config.getMapStoreConfig().isEnabled();
            }
            return false;
        }).count();
    }

    private long countMapWithAtleastOneQueryCache(Node node) {
        return maps.stream().filter(distributedObject -> {
            MapConfig config = node.getConfig().getMapConfig(distributedObject.getName());
            if (config != null) {
                return !config.getQueryCacheConfigs().isEmpty();
            }
            return false;
        }).count();

    }
}

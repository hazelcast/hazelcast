/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.cp.internal.datastructures.cpmap.store.HeapCPMapStore;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages the mapping of a CP group's constituent object names and their respective {@link CPMapStore}, in addition to the set
 * of destroyed {@link CPMap} name's relative to the respective CP group. The caller does not need to use
 * concurrency control when interacting with a CPMapRegistry -- all interactions should be occurring exclusively via the
 * respective CP group's thread.
 */
public class CPMapRegistry {
    /**
     * Default maximum size in MB for a {@link CPMap}.
     * This is the total amount that the key-value pairs
     * of a {@link CPMap} can total.
     */
    public static final int DEFAULT_MAP_MAX_SIZE_MB = 100;

    private final String groupName;
    private final ConcurrentMap<String, CPMapStore> mapStores;
    private final Set<String> destroyed;

    public CPMapRegistry(CPGroupId groupId) {
        this.groupName = groupId.getName();
        this.mapStores = new ConcurrentHashMap<>();
        this.destroyed = ConcurrentHashMap.newKeySet();
    }

    public CPMapStore getMapStore(NodeEngine nodeEngine, String mapName, CPSubsystemConfig cpSubsystemConfig) {
        final CPMapStore existing = mapStores.get(mapName);
        if (existing != null) {
            return existing;
        }

        if (destroyed.contains(mapName)) {
            throw new DistributedObjectDestroyedException(
                    "CPMap[" + mapName + "@" + groupName + "] is already destroyed!"
            );
        }

        return mapStores.computeIfAbsent(mapName,
                k -> {
                    return createHeapCPMapStore(nodeEngine, cpSubsystemConfig, mapName);
                });
    }

    protected HeapCPMapStore createHeapCPMapStore(NodeEngine nodeEngine,
                                                  CPSubsystemConfig cpSubsystemConfig,
                                                  String mapName) {
        CPMapConfig mapConfig = cpSubsystemConfig.findCPMapConfig(mapName);
        return new HeapCPMapStore(getMapMaxSizeMb(mapConfig));
    }

    public boolean destroyMapStore(String mapName) {
        destroyed.add(mapName);
        return mapStores.remove(mapName) != null;
    }

    public void collectMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        for (Map.Entry<String, CPMapStore> e : mapStores.entrySet()) {
            String mapName = e.getKey();
            CPMapStore cpMapStore = e.getValue();

            MetricDescriptor mapDescriptorRoot =
                    descriptor.copy()
                            .withDiscriminator("id", mapName + "@" + groupName)
                            .withTag("name", mapName)
                            .withTag("group", groupName);
            cpMapStore.collectMetrics(mapDescriptorRoot, context);
        }
    }

    static int getMapMaxSizeMb(CPMapConfig mapConfig) {
        // The way configs are found is pretty rudimentary IMO -- we always strip the group name, hence just using objectName.
        // This implies a map name is unique across all CP groups -- see the other configs in CP and the use of getBaseName(..)
        // in their findXxx methods; this is why we don't even care about CPGroupId here.
        if (mapConfig != null) {
            return mapConfig.getMaxSizeMb();
        }
        return DEFAULT_MAP_MAX_SIZE_MB;
    }

    String getGroupName() {
        return groupName;
    }

    Set<String> getDestroyed() {
        return destroyed;
    }

    Map<String, CPMapStore> getMapStores() {
        return mapStores;
    }

    public void resetForChunkedSnapshotRestore() {
        mapStores.clear();
        destroyed.clear();
    }

    public CPMapRegistrySnapshot takeSnapshot() {
        return new CPMapRegistrySnapshot(this);
    }
}

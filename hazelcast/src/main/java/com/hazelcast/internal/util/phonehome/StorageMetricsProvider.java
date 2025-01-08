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

import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.stream.Stream;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DATA_MEMORY_COST;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.HD_MEMORY_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_FREE_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_NATIVE_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIERED_STORAGE_ENABLED;

class StorageMetricsProvider implements MetricsProvider {

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        memory(node, context);
        tieredStorage(node.getConfig(), context);
        dataMemoryCost(node.getNodeEngine(), context);
    }

    private void memory(Node node, MetricsCollectionContext context) {
        NativeMemoryConfig nativeMemoryConfig = node.getConfig().getNativeMemoryConfig();

        boolean isHdEnabled = nativeMemoryConfig.isEnabled();
        context.collect(HD_MEMORY_ENABLED, isHdEnabled);

        MemoryStats memoryStats = node.getNodeExtension().getMemoryStats();
        if (isHdEnabled) {
            context.collect(MEMORY_USED_NATIVE_SIZE, memoryStats.getUsedNative());
        }
        context.collect(MEMORY_USED_HEAP_SIZE, memoryStats.getUsedHeap());
        context.collect(MEMORY_FREE_HEAP_SIZE, memoryStats.getFreeHeap());
    }

    private void tieredStorage(Config config, MetricsCollectionContext context) {
        boolean tieredStorageEnabled = config.getMapConfigs().values().stream()
                .anyMatch(mapConfig -> mapConfig.getTieredStoreConfig().isEnabled());
        context.collect(TIERED_STORAGE_ENABLED, tieredStorageEnabled);
    }

    private void dataMemoryCost(NodeEngine nodeEngine, MetricsCollectionContext context) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        ReplicatedMapService replicatedMapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        long totalEntryMemoryCost =
                Stream.concat(
                        mapService.getStats().values().stream(),
                        replicatedMapService.getStats().values().stream())
                .mapToLong(LocalMapStats::getOwnedEntryMemoryCost)
                .sum();
        context.collect(DATA_MEMORY_COST, totalEntryMemoryCost);
    }
}

/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DATA_MEMORY_COST;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.HD_MEMORY_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_FREE_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_NATIVE_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIERED_STORAGE_ENABLED;

public class StorageInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        Config config = nodeEngine.getConfig();

        memory(metricsConsumer, config, node.getNodeExtension());
        tieredStorage(metricsConsumer, config);
        dataMemoryCost(metricsConsumer, nodeEngine);
    }

    private void memory(
            BiConsumer<PhoneHomeMetrics, String> metricsConsumer,
            Config config, NodeExtension nodeExtension
    ) {
        NativeMemoryConfig nativeMemoryConfig = config.getNativeMemoryConfig();

        boolean isHdEnabled = nativeMemoryConfig.isEnabled();
        metricsConsumer.accept(HD_MEMORY_ENABLED, String.valueOf(isHdEnabled));

        MemoryStats memoryStats = nodeExtension.getMemoryStats();
        if (isHdEnabled) {
            long offHeapMemorySize = memoryStats.getUsedNative();
            metricsConsumer.accept(MEMORY_USED_NATIVE_SIZE, String.valueOf(offHeapMemorySize));
        }
        metricsConsumer.accept(MEMORY_USED_HEAP_SIZE, String.valueOf(memoryStats.getUsedHeap()));
        metricsConsumer.accept(MEMORY_FREE_HEAP_SIZE, String.valueOf(memoryStats.getFreeHeap()));
    }

    private void tieredStorage(BiConsumer<PhoneHomeMetrics, String> metricsConsumer, Config config) {
        boolean tieredStorageEnabled = config.getMapConfigs().values().stream()
                .anyMatch(mapConfig -> mapConfig.getTieredStoreConfig().isEnabled());
        metricsConsumer.accept(TIERED_STORAGE_ENABLED, String.valueOf(tieredStorageEnabled));
    }

    private void dataMemoryCost(BiConsumer<PhoneHomeMetrics, String> metricsConsumer, NodeEngine nodeEngine) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        ReplicatedMapService replicatedMapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        long totalEntryMemoryCost = Stream.concat(
                        mapService.getStats().values().stream(),
                        replicatedMapService.getStats().values().stream()
                ).mapToLong(LocalMapStats::getOwnedEntryMemoryCost)
                .sum();
        metricsConsumer.accept(DATA_MEMORY_COST, String.valueOf(totalEntryMemoryCost));
    }
}

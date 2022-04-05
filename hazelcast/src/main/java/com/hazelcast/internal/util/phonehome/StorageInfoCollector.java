/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class StorageInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        Config config = node.getNodeEngine().getConfig();
        NativeMemoryConfig nativeMemoryConfig = config.getNativeMemoryConfig();
        boolean isHdEnabled = nativeMemoryConfig.isEnabled();
        metricsConsumer.accept(PhoneHomeMetrics.HD_MEMORY_ENABLED, String.valueOf(isHdEnabled));
        if (isHdEnabled) {
            long offHeapMemorySize = node.getNodeExtension().getMemoryStats().getUsedNative();
            metricsConsumer.accept(PhoneHomeMetrics.MEMORY_USED_NATIVE_SIZE, String.valueOf(offHeapMemorySize));
        }
        long usedHeap = node.getNodeExtension().getMemoryStats().getUsedHeap();
        metricsConsumer.accept(PhoneHomeMetrics.MEMORY_USED_HEAP_SIZE, String.valueOf(usedHeap));
        boolean tieredStorageEnabled = config.getMapConfigs().values().stream()
                .anyMatch(mapConfig -> mapConfig.getTieredStoreConfig().isEnabled());
        metricsConsumer.accept(PhoneHomeMetrics.TIERED_STORAGE_ENABLED, String.valueOf(tieredStorageEnabled));

        MapService mapService = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        ReplicatedMapService replicatedMapService = node.getNodeEngine().getService(ReplicatedMapService.SERVICE_NAME);
        long totalEntryMemoryCost = Stream.concat(
                        mapService.getStats().values().stream(),
                        replicatedMapService.getStats().values().stream()
                ).mapToLong(LocalMapStats::getOwnedEntryMemoryCost)
                .sum();
        metricsConsumer.accept(PhoneHomeMetrics.DATA_MEMORY_COST, String.valueOf(totalEntryMemoryCost));
    }
}

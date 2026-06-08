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

package com.hazelcast.vector.impl.stats;

import com.hazelcast.internal.memory.Measurable;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.JVMUtil;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.vector.impl.service.VectorCollectionServiceImpl;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PREFIX;

public class LocalVectorCollectionStatsProvider implements DynamicMetricsProvider, Measurable {
    public static final long FIXED_HEAP_BYTES_USED = JVMUtil.OBJECT_HEADER_SIZE + 3L * JVMUtil.REFERENCE_COST_IN_BYTES
            // constructorFunction capture
            + JVMUtil.OBJECT_HEADER_SIZE + JVMUtil.REFERENCE_COST_IN_BYTES;
    // statsMap entry: key reference (shared string), value reference, value memory
    public static final long ENTRY_HEAP_BYTES_USED = JVMUtil.REFERENCE_COST_IN_BYTES * 2L
            + LocalVectorCollectionStatsImpl.FIXED_HEAP_BYTES_USED;

    private final ConcurrentMap<String, LocalVectorCollectionStatsImpl> statsMap;

    private VectorCollectionServiceImpl service;

    private final ConstructorFunction<String, LocalVectorCollectionStatsImpl> constructorFunction =
            name -> {
                var backupCount = service.getExistingVectorCollectionConfig(name).getTotalBackupCount();
                var localVectorCollectionStats = new LocalVectorCollectionStatsImpl();
                localVectorCollectionStats.setBackupCount(backupCount);
                return localVectorCollectionStats;
            };

    public LocalVectorCollectionStatsProvider(NodeEngine nodeEngine, VectorCollectionServiceImpl service) {
        this.service = service;
        this.statsMap = MapUtil.createConcurrentHashMap(nodeEngine.getConfig().getVectorCollectionConfigs().size());
    }

    public LocalVectorCollectionStatsImpl getStatistics(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public boolean hasStatistics(String name) {
        return statsMap.containsKey(name);
    }

    public void destroyStatistics(String name) {
        statsMap.remove(name);
    }

    public void reset() {
        statsMap.clear();
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        for (var entry : statsMap.entrySet()) {
            String name = entry.getKey();
            var localInstanceStats = entry.getValue();

            // enrich with general, on-demand stats
            var onDemandStats = service.getOnDemandStats(name);

            // total heap cost includes collection-wise costs and is larger than owned+backup heap cost
            localInstanceStats.setHeapCost(onDemandStats.heapBytesUsed());

            localInstanceStats.setOwnedEntryCount(onDemandStats.owned().size());
            localInstanceStats.setOwnedEntryHeapMemoryCost(onDemandStats.owned().heapBytesUsed());

            localInstanceStats.setBackupEntryCount(onDemandStats.backup().size());
            localInstanceStats.setBackupEntryHeapMemoryCost(onDemandStats.backup().heapBytesUsed());

            localInstanceStats.setVectorIndexStats(onDemandStats.getVectorIndexStats());

            // collections
            MetricDescriptor dsDescriptor = descriptor
                    .copy()
                    .withPrefix(VECTOR_COLLECTION_PREFIX)
                    .withDiscriminator(VECTOR_COLLECTION_DISCRIMINATOR_NAME, name);
            context.collect(dsDescriptor, localInstanceStats);
        }
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED
                + statsMap.size() * ENTRY_HEAP_BYTES_USED;
    }
}

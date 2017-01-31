/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.utils;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;

import java.util.Collections;
import java.util.Map;

/**
 * Various utility methods used in order to easily access {@code QueryCacheContext} internals.
 */
public final class QueryCacheUtil {

    private QueryCacheUtil() {
    }

    /**
     * Returns accumulators of a {@code QueryCache}.
     */
    public static Map<Integer, Accumulator> getAccumulators(QueryCacheContext context, String mapName, String cacheName) {
        PartitionAccumulatorRegistry partitionAccumulatorRegistry = getAccumulatorRegistryOrNull(context, mapName, cacheName);
        if (partitionAccumulatorRegistry == null) {
            return Collections.emptyMap();
        }
        return partitionAccumulatorRegistry.getAll();
    }

    /**
     * Returns {@code PartitionAccumulatorRegistry} of a {@code QueryCache}.
     *
     * @see PartitionAccumulatorRegistry
     */
    public static PartitionAccumulatorRegistry getAccumulatorRegistryOrNull(QueryCacheContext context,
                                                                            String mapName, String cacheName) {
        PublisherContext publisherContext = context.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
        if (publisherRegistry == null) {
            return null;
        }
        return publisherRegistry.getOrNull(cacheName);
    }

    /**
     * Returns {@code Accumulator} of a partition.
     *
     * @see Accumulator
     */
    public static Accumulator getAccumulatorOrNull(QueryCacheContext context,
                                                   String mapName, String cacheName, int partitionId) {
        PartitionAccumulatorRegistry accumulatorRegistry = getAccumulatorRegistryOrNull(context, mapName, cacheName);
        if (accumulatorRegistry == null) {
            return null;
        }
        return accumulatorRegistry.getOrNull(partitionId);
    }
}

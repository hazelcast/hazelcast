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

package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorFactory;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Registry of mappings like {@code cacheId} to {@code PartitionAccumulatorRegistry}.
 *
 * @see PartitionAccumulatorRegistry
 */
public class PublisherRegistry implements Registry<String, PartitionAccumulatorRegistry> {

    private final ConstructorFunction<String, PartitionAccumulatorRegistry> partitionAccumulatorRegistryConstructor =
            cacheId -> {
                AccumulatorInfo info = getAccumulatorInfo(cacheId);
                checkNotNull(info, "info cannot be null");

                AccumulatorFactory accumulatorFactory = createPublisherAccumulatorFactory();
                ConstructorFunction<Integer, Accumulator> constructor
                        = new PublisherAccumulatorConstructor(info, accumulatorFactory);
                return new PartitionAccumulatorRegistry(info, constructor);
            };

    private final String mapName;
    private final QueryCacheContext context;
    private final ConcurrentMap<String, PartitionAccumulatorRegistry> partitionAccumulators;

    public PublisherRegistry(QueryCacheContext context, String mapName) {
        this.context = context;
        this.mapName = mapName;
        this.partitionAccumulators = new ConcurrentHashMap<>();
    }

    @Override
    public PartitionAccumulatorRegistry getOrCreate(String cacheId) {
        return getOrPutIfAbsent(partitionAccumulators, cacheId, partitionAccumulatorRegistryConstructor);
    }

    @Override
    public PartitionAccumulatorRegistry getOrNull(String cacheId) {
        return partitionAccumulators.get(cacheId);
    }

    @Override
    public Map<String, PartitionAccumulatorRegistry> getAll() {
        return Collections.unmodifiableMap(partitionAccumulators);
    }

    @Override
    public PartitionAccumulatorRegistry remove(String cacheId) {
        return partitionAccumulators.remove(cacheId);
    }

    /**
     * Constructor used to create a publisher accumulator.
     */
    private static class PublisherAccumulatorConstructor implements ConstructorFunction<Integer, Accumulator> {

        private final AccumulatorInfo info;

        private final AccumulatorFactory accumulatorFactory;

        PublisherAccumulatorConstructor(AccumulatorInfo info, AccumulatorFactory accumulatorFactory) {
            this.info = info;
            this.accumulatorFactory = accumulatorFactory;
        }

        @Override
        public Accumulator createNew(Integer partitionId) {
            return accumulatorFactory.createAccumulator(info);
        }

    }

    private AccumulatorInfo getAccumulatorInfo(String cacheId) {
        PublisherContext publisherContext = context.getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        return infoSupplier.getAccumulatorInfoOrNull(mapName, cacheId);
    }

    private PublisherAccumulatorFactory createPublisherAccumulatorFactory() {
        return new PublisherAccumulatorFactory(context);
    }
}

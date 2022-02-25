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

import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of mappings like {@code partitionId} to {@code Accumulator}.
 * Holds all partition accumulators of a {@link com.hazelcast.map.QueryCache QueryCache}.
 *
 * @see Accumulator
 */
public class PartitionAccumulatorRegistry implements Registry<Integer, Accumulator> {

    private final EventFilter eventFilter;
    private final AccumulatorInfo info;
    private final ConcurrentMap<Integer, Accumulator> accumulators;
    private final ConstructorFunction<Integer, Accumulator> accumulatorConstructor;
    /**
     * UUID of subscriber client/node.
     */
    private volatile UUID uuid;

    public PartitionAccumulatorRegistry(AccumulatorInfo info,
                                        ConstructorFunction<Integer, Accumulator> accumulatorConstructor) {
        this.info = info;
        this.eventFilter = createEventFilter();
        this.accumulatorConstructor = accumulatorConstructor;
        this.accumulators = new ConcurrentHashMap<>();
    }

    private EventFilter createEventFilter() {
        boolean includeValue = info.isIncludeValue();
        Predicate predicate = info.getPredicate();
        return new QueryEventFilter(null, predicate, includeValue);
    }

    @Override
    public Accumulator getOrCreate(Integer partitionId) {
        return ConcurrencyUtil.getOrPutIfAbsent(accumulators, partitionId, accumulatorConstructor);
    }

    @Override
    public Accumulator getOrNull(Integer partitionId) {
        return accumulators.get(partitionId);
    }

    @Override
    public Map<Integer, Accumulator> getAll() {
        return Collections.unmodifiableMap(accumulators);
    }

    @Override
    public Accumulator remove(Integer id) {
        return accumulators.remove(id);
    }

    public EventFilter getEventFilter() {
        return eventFilter;
    }

    public AccumulatorInfo getInfo() {
        return info;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }
}

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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.IMap;
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
 * All registered subscriber side accumulators for an {@link IMap IMap}
 * can be reached from this registry class.
 * <p>
 * Every map has only one {@link SubscriberRegistry}.
 */
public class SubscriberRegistry implements Registry<String, Accumulator> {

    private final ConstructorFunction<String, Accumulator> accumulatorConstructor =
            cacheId -> {
                AccumulatorInfo info = getAccumulatorInfo(cacheId);
                checkNotNull(info, "info cannot be null");

                AccumulatorFactory accumulatorFactory = createSubscriberAccumulatorFactory();
                return accumulatorFactory.createAccumulator(info);
            };

    private final String mapName;
    private final QueryCacheContext context;
    private final ConcurrentMap<String, Accumulator> accumulators;

    public SubscriberRegistry(QueryCacheContext context, String mapName) {
        this.context = context;
        this.mapName = mapName;
        this.accumulators = new ConcurrentHashMap<>();
    }

    @Override
    public Accumulator getOrCreate(String cacheId) {
        return getOrPutIfAbsent(accumulators, cacheId, accumulatorConstructor);
    }

    @Override
    public Accumulator getOrNull(String cacheId) {
        return accumulators.get(cacheId);
    }

    @Override
    public Map<String, Accumulator> getAll() {
        return Collections.unmodifiableMap(accumulators);
    }

    @Override
    public Accumulator remove(String cacheId) {
        return accumulators.remove(cacheId);
    }

    private AccumulatorInfo getAccumulatorInfo(String cacheId) {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        AccumulatorInfoSupplier infoSupplier = subscriberContext.getAccumulatorInfoSupplier();
        return infoSupplier.getAccumulatorInfoOrNull(mapName, cacheId);
    }

    protected SubscriberAccumulatorFactory createSubscriberAccumulatorFactory() {
        return new SubscriberAccumulatorFactory(context);
    }

    protected QueryCacheContext getContext() {
        return context;
    }
}

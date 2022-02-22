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

import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.accumulator.DefaultAccumulatorInfoSupplier;

/**
 * Abstract implementation of {@code SubscriberContext}.
 * This class is used by both client and node parts.
 *
 * @see SubscriberContext
 */
public abstract class AbstractSubscriberContext implements SubscriberContext {

    private final QueryCacheEventService eventService;
    private final QueryCacheEndToEndProvider queryCacheEndToEndProvider;
    private final MapSubscriberRegistry mapSubscriberRegistry;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final QueryCacheFactory queryCacheFactory;
    private final DefaultAccumulatorInfoSupplier accumulatorInfoSupplier;

    public AbstractSubscriberContext(QueryCacheContext context) {
        this.queryCacheConfigurator = context.getQueryCacheConfigurator();
        this.eventService = context.getQueryCacheEventService();
        this.queryCacheEndToEndProvider = new QueryCacheEndToEndProvider(context.getLifecycleMutexFactory());
        this.mapSubscriberRegistry = new MapSubscriberRegistry(context);
        this.queryCacheFactory = new QueryCacheFactory();
        this.accumulatorInfoSupplier = new DefaultAccumulatorInfoSupplier();
    }

    @Override
    public QueryCacheEndToEndProvider getEndToEndQueryCacheProvider() {
        return queryCacheEndToEndProvider;
    }

    @Override
    public MapSubscriberRegistry getMapSubscriberRegistry() {
        return mapSubscriberRegistry;
    }

    @Override
    public QueryCacheFactory getQueryCacheFactory() {
        return queryCacheFactory;
    }

    @Override
    public AccumulatorInfoSupplier getAccumulatorInfoSupplier() {
        return accumulatorInfoSupplier;
    }

    @Override
    public QueryCacheEventService getEventService() {
        return eventService;
    }

    @Override
    public QueryCacheConfigurator geQueryCacheConfigurator() {
        return queryCacheConfigurator;
    }
}

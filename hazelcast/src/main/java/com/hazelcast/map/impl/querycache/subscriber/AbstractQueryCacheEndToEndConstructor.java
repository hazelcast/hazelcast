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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;

import static com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo.createAccumulatorInfo;
import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Provides generic functionality for {@code QueryCacheEndToEndConstructor} implementations.
 *
 * @see QueryCacheEndToEndConstructor
 */
public abstract class AbstractQueryCacheEndToEndConstructor implements QueryCacheEndToEndConstructor {

    protected static final int OPERATION_WAIT_TIMEOUT_MINUTES = 5;

    protected final String mapName;
    protected final QueryCacheRequest request;
    protected final QueryCacheContext context;
    protected final SubscriberContext subscriberContext;
    protected final ILogger logger = Logger.getLogger(getClass());

    protected Predicate predicate;
    protected boolean includeValue;
    protected InternalQueryCache queryCache;

    public AbstractQueryCacheEndToEndConstructor(QueryCacheRequest request) {
        this.request = request;
        this.mapName = request.getMapName();
        this.context = request.getContext();
        this.subscriberContext = context.getSubscriberContext();
    }

    @Override
    public final void createSubscriberAccumulator(AccumulatorInfo info) {
        QueryCacheEventService eventService = context.getQueryCacheEventService();
        ListenerAdapter listener = new SubscriberListener(context, info);
        eventService.listenPublisher(info.getMapName(), info.getCacheName(), listener);
    }

    /**
     * Here order of method calls should not change.
     */
    @Override
    public final InternalQueryCache createNew(String arg) {
        try {
            QueryCacheConfig queryCacheConfig = initQueryCacheConfig(request);
            if (queryCacheConfig == null) {
                return NULL_QUERY_CACHE;
            }
            queryCache = createUnderlyingQueryCache(request);
            // this is users listener which can be given as a parameter
            // when calling `IMap.getQueryCache` method
            addListener(request);

            AccumulatorInfo info = createAccumulatorInfo(queryCacheConfig, mapName, request.getCacheName(), predicate);
            addInfoToSubscriberContext(info);

            info.setPublishable(true);

            createSubscriberAccumulator(info);
            createPublisherAccumulator(info);

        } catch (Throwable throwable) {
            removeQueryCacheConfig(mapName, request.getUserGivenCacheName());
            throw rethrow(throwable);
        }

        return queryCache;
    }

    /**
     * This is the cache which we store all key-value pairs.
     */
    private InternalQueryCache createUnderlyingQueryCache(QueryCacheRequest request) {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();
        return queryCacheFactory.create(request);
    }

    private void addInfoToSubscriberContext(AccumulatorInfo info) {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        AccumulatorInfoSupplier accumulatorInfoSupplier = subscriberContext.getAccumulatorInfoSupplier();
        accumulatorInfoSupplier.putIfAbsent(info.getMapName(), info.getCacheName(), info);
    }

    private String addListener(QueryCacheRequest request) {
        MapListener listener = request.getListener();
        if (listener == null) {
            return null;
        }
        QueryCacheEventService eventService = subscriberContext.getEventService();
        return eventService.addListener(request.getMapName(), request.getCacheName(), listener);
    }

    public String getCacheName() {
        return request.getCacheName();
    }

    protected Object toObject(Object data) {
        return context.toObject(data);
    }

    protected QueryCacheConfig initQueryCacheConfig(QueryCacheRequest request) {
        Predicate predicate = request.getPredicate();

        QueryCacheConfig queryCacheConfig;

        if (predicate == null) {
            queryCacheConfig = getOrNullQueryCacheConfig(mapName, request.getUserGivenCacheName());
        } else {
            queryCacheConfig = getOrCreateQueryCacheConfig(mapName, request.getUserGivenCacheName());
            queryCacheConfig.setIncludeValue(request.isIncludeValue());
            queryCacheConfig.getPredicateConfig().setImplementation(predicate);
        }

        if (queryCacheConfig == null) {
            return null;
        }

        // init some required parameters
        this.includeValue = queryCacheConfig.isIncludeValue();
        this.predicate = queryCacheConfig.getPredicateConfig().getImplementation();

        return queryCacheConfig;
    }

    private QueryCacheConfig getOrCreateQueryCacheConfig(String mapName, String cacheName) {
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        return queryCacheConfigurator.getOrCreateConfiguration(mapName, cacheName);
    }

    private QueryCacheConfig getOrNullQueryCacheConfig(String mapName, String cacheName) {
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        return queryCacheConfigurator.getOrNull(mapName, cacheName);
    }

    private void removeQueryCacheConfig(String mapName, String cacheName) {
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        queryCacheConfigurator.removeConfiguration(mapName, cacheName);
    }
}

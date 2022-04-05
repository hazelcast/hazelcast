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

import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.query.Predicate;

import java.util.UUID;

import static com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo.toAccumulatorInfo;
import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

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

    protected InternalQueryCache queryCache;

    private Predicate predicate;
    private UUID publisherListenerId;

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
        publisherListenerId = eventService.addPublisherListener(info.getMapName(), info.getCacheId(), listener);
        queryCache.setPublisherListenerId(publisherListenerId);
    }

    /**
     * Here order of method calls should not change.
     */
    @Override
    public final InternalQueryCache createNew(String cacheId) {
        try {
            QueryCacheConfig queryCacheConfig = initQueryCacheConfig(request, cacheId);
            if (queryCacheConfig == null) {
                // no matching configuration was found, `null` will
                // be returned to user from IMap#getQueryCache call.
                return NULL_QUERY_CACHE;
            }
            queryCache = createUnderlyingQueryCache(queryCacheConfig, request, cacheId);

            AccumulatorInfo info = toAccumulatorInfo(queryCacheConfig, mapName, cacheId, predicate);
            addInfoToSubscriberContext(info);

            info.setPublishable(true);

            UUID publisherListenerId = queryCache.getPublisherListenerId();
            if (publisherListenerId == null) {
                createSubscriberAccumulator(info);
            }
            createPublisherAccumulator(info, request.isUrgent());

        } catch (Throwable throwable) {
            removeQueryCacheConfig(mapName, request.getCacheName());
            throw rethrow(throwable);
        }

        return queryCache;
    }

    /**
     * This is the cache which we store all key-value pairs.
     */
    private InternalQueryCache createUnderlyingQueryCache(QueryCacheConfig queryCacheConfig,
                                                          QueryCacheRequest request,
                                                          String cacheId) {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();
        request.withQueryCacheConfig(queryCacheConfig);
        return queryCacheFactory.create(request, cacheId);
    }

    private void addInfoToSubscriberContext(AccumulatorInfo info) {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        AccumulatorInfoSupplier accumulatorInfoSupplier = subscriberContext.getAccumulatorInfoSupplier();
        accumulatorInfoSupplier.putIfAbsent(info.getMapName(), info.getCacheId(), info);
    }

    protected Object toObject(Object data) {
        return context.toObject(data);
    }

    private QueryCacheConfig initQueryCacheConfig(QueryCacheRequest request, String cacheId) {
        Predicate predicate = request.getPredicate();

        QueryCacheConfig queryCacheConfig;

        if (predicate == null) {
            // user called IMap#getQueryCache method only providing
            // a name (but without a predicate), here we are trying
            // to find a matching configuration for this query cache.
            queryCacheConfig = getOrNullQueryCacheConfig(mapName, request.getCacheName(), cacheId);
        } else {
            queryCacheConfig = getOrCreateQueryCacheConfig(mapName, request.getCacheName(), cacheId);
            queryCacheConfig.setIncludeValue(request.isIncludeValue());
            queryCacheConfig.getPredicateConfig().setImplementation(predicate);
        }

        if (queryCacheConfig == null) {
            return null;
        }

        this.predicate = queryCacheConfig.getPredicateConfig().getImplementation();

        return queryCacheConfig;
    }

    private QueryCacheConfig getOrCreateQueryCacheConfig(String mapName, String cacheName, String cacheId) {
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        return queryCacheConfigurator.getOrCreateConfiguration(mapName, cacheName, cacheId);
    }

    private QueryCacheConfig getOrNullQueryCacheConfig(String mapName, String cacheName, String cacheId) {
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        return queryCacheConfigurator.getOrNull(mapName, cacheName, cacheId);
    }

    private void removeQueryCacheConfig(String mapName, String cacheName) {
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        queryCacheConfigurator.removeConfiguration(mapName, cacheName);
    }
}

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
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;

/**
 * Context contract for subscriber side of a {@link com.hazelcast.map.QueryCache QueryCache} implementation.
 */
public interface SubscriberContext {

    /**
     * Returns {@link QueryCacheEventService} for this context.
     *
     * @return {@link QueryCacheEventService} for this context.
     * @see QueryCacheEventService
     */
    QueryCacheEventService getEventService();

    /**
     * Returns {@link QueryCacheEndToEndProvider} for this context.
     *
     * @return {@link QueryCacheEndToEndProvider} for this context.
     * @see QueryCacheEndToEndProvider
     */
    QueryCacheEndToEndProvider getEndToEndQueryCacheProvider();

    /**
     * Returns {@link QueryCacheConfigurator} for this context.
     *
     * @return {@link QueryCacheConfigurator} for this context.
     * @see QueryCacheConfigurator
     */
    QueryCacheConfigurator geQueryCacheConfigurator();


    /**
     * Returns {@link QueryCacheFactory} for this context.
     *
     * @return {@link QueryCacheFactory} for this context.
     * @see QueryCacheFactory
     */
    QueryCacheFactory getQueryCacheFactory();

    /**
     * Returns {@link AccumulatorInfoSupplier} for this context.
     *
     * @return {@link AccumulatorInfoSupplier} for this context.
     * @see AccumulatorInfoSupplier
     */
    AccumulatorInfoSupplier getAccumulatorInfoSupplier();

    /**
     * Returns {@link MapSubscriberRegistry} for this context.
     *
     * @return {@link MapSubscriberRegistry} for this context.
     * @see MapSubscriberRegistry
     */
    MapSubscriberRegistry getMapSubscriberRegistry();

    /**
     * Returns {@link SubscriberContextSupport} for this context.
     *
     * @return {@link SubscriberContextSupport} for this context.
     * @see SubscriberContextSupport
     */
    SubscriberContextSupport getSubscriberContextSupport();

    /**
     * @param request see {@link QueryCacheRequest}
     * @return a new instance of {@link QueryCacheEndToEndConstructor}
     */
    QueryCacheEndToEndConstructor newEndToEndConstructor(QueryCacheRequest request);
}

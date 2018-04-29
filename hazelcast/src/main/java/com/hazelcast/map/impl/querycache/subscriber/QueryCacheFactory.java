/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Static factory for simple {@link com.hazelcast.map.QueryCache QueryCache} implementations.
 */
public class QueryCacheFactory {

    /**
     * Constructor for an instance of {@link com.hazelcast.map.QueryCache QueryCache}.
     */
    private static class InternalQueryCacheConstructor implements ConstructorFunction<String, InternalQueryCache> {

        private final QueryCacheRequest request;

        InternalQueryCacheConstructor(QueryCacheRequest request) {
            this.request = request;
        }

        @Override
        public InternalQueryCache createNew(String cacheId) {
            String cacheName = request.getCacheName();
            IMap delegate = request.getMap();
            QueryCacheContext context = request.getContext();

            return new DefaultQueryCache(cacheId, cacheName, delegate, context);
        }
    }

    private final ConcurrentMap<String, InternalQueryCache> internalQueryCaches;

    public QueryCacheFactory() {
        this.internalQueryCaches = new ConcurrentHashMap<String, InternalQueryCache>();
    }

    public InternalQueryCache create(QueryCacheRequest request, String cacheId) {
        return ConcurrencyUtil.getOrPutIfAbsent(internalQueryCaches,
                cacheId, new InternalQueryCacheConstructor(request));
    }

    public boolean remove(InternalQueryCache queryCache) {
        return internalQueryCaches.remove(queryCache.getCacheId(), queryCache);
    }

    public InternalQueryCache getOrNull(String cacheId) {
        return internalQueryCaches.get(cacheId);
    }

    // only used for testing
    public int getQueryCacheCount() {
        return internalQueryCaches.size();
    }
}

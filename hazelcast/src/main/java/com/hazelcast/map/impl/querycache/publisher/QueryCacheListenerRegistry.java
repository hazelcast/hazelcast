/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IFunction;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds IDs of registered listeners which are registered to listen underlying
 * {@code IMap} events to feed {@link com.hazelcast.map.QueryCache QueryCache}.
 * <p>
 * This class contains mappings like: cacheId ---> registered listener IDs for underlying {@code IMap}.
 */
public class QueryCacheListenerRegistry implements Registry<String, String> {

    private final ConstructorFunction<String, String> registryConstructorFunction =
            new ConstructorFunction<String, String>() {
                @Override
                public String createNew(String ignored) {
                    IFunction<String, String> registration = context.getPublisherContext().getListenerRegistrator();
                    return registration.apply(mapName);
                }
            };

    private final String mapName;
    private final QueryCacheContext context;
    private final ConcurrentMap<String, String> listeners;

    public QueryCacheListenerRegistry(QueryCacheContext context, String mapName) {
        this.context = context;
        this.mapName = mapName;
        this.listeners = new ConcurrentHashMap<String, String>();
    }

    @Override
    public String getOrCreate(String cacheId) {
        return ConcurrencyUtil.getOrPutIfAbsent(listeners, cacheId, registryConstructorFunction);
    }

    @Override
    public String getOrNull(String cacheId) {
        return listeners.get(cacheId);
    }

    @Override
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(listeners);
    }

    @Override
    public String remove(String cacheId) {
        return listeners.remove(cacheId);
    }
}

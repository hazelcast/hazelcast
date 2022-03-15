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
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds {@link QueryCacheListenerRegistry} for each {@code IMap}.
 *
 * @see QueryCacheListenerRegistry
 */
public class MapListenerRegistry implements Registry<String, QueryCacheListenerRegistry> {

    private final ConstructorFunction<String, QueryCacheListenerRegistry> registryConstructorFunction =
            new ConstructorFunction<String, QueryCacheListenerRegistry>() {
                @Override
                public QueryCacheListenerRegistry createNew(String mapName) {
                    return new QueryCacheListenerRegistry(context, mapName);
                }
            };

    private final QueryCacheContext context;
    private final ConcurrentMap<String, QueryCacheListenerRegistry> listeners;

    public MapListenerRegistry(QueryCacheContext context) {
        this.listeners = new ConcurrentHashMap<>();
        this.context = context;
    }

    @Override
    public QueryCacheListenerRegistry getOrCreate(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(listeners, mapName, registryConstructorFunction);
    }

    @Override
    public QueryCacheListenerRegistry getOrNull(String mapName) {
        return listeners.get(mapName);
    }

    @Override
    public Map<String, QueryCacheListenerRegistry> getAll() {
        return Collections.unmodifiableMap(listeners);
    }

    @Override
    public QueryCacheListenerRegistry remove(String id) {
        return listeners.remove(id);
    }
}

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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Holds IDs of registered listeners which are registered to listen underlying
 * {@code IMap} events to feed {@link com.hazelcast.map.QueryCache QueryCache}.
 * <p>
 * This class contains mappings like: cacheId ---&gt; registered listener IDs for underlying {@code IMap}.
 */
public class QueryCacheListenerRegistry implements Registry<String, UUID> {

    private final ConstructorFunction<String, UUID> registryConstructorFunction =
            new ConstructorFunction<String, UUID>() {
                @Override
                public UUID createNew(String ignored) {
                    Function<String, UUID> registration = context.getPublisherContext().getListenerRegistrator();
                    return registration.apply(mapName);
                }
            };

    private final String mapName;
    private final QueryCacheContext context;
    private final ConcurrentMap<String, UUID> listeners;

    public QueryCacheListenerRegistry(QueryCacheContext context, String mapName) {
        this.context = context;
        this.mapName = mapName;
        this.listeners = new ConcurrentHashMap<>();
    }

    @Override
    public UUID getOrCreate(String cacheId) {
        return ConcurrencyUtil.getOrPutIfAbsent(listeners, cacheId, registryConstructorFunction);
    }

    @Override
    public UUID getOrNull(String cacheId) {
        return listeners.get(cacheId);
    }

    @Override
    public Map<String, UUID> getAll() {
        return Collections.unmodifiableMap(listeners);
    }

    @Override
    public UUID remove(String cacheId) {
        return listeners.remove(cacheId);
    }
}

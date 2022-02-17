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
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Registry of mappings like {@code mapName} to {@code PublisherRegistry}.
 *
 * @see PublisherRegistry
 */
public class MapPublisherRegistry implements Registry<String, PublisherRegistry> {

    private final ConstructorFunction<String, PublisherRegistry> registryConstructorFunction =
            this::createPublisherRegistry;

    private final QueryCacheContext context;
    private final ConcurrentMap<String, PublisherRegistry> cachePublishersPerIMap;

    public MapPublisherRegistry(QueryCacheContext context) {
        this.context = context;
        this.cachePublishersPerIMap = new ConcurrentHashMap<>();
    }

    @Override
    public PublisherRegistry getOrCreate(String mapName) {
        return getOrPutIfAbsent(cachePublishersPerIMap, mapName, registryConstructorFunction);
    }

    @Override
    public PublisherRegistry getOrNull(String mapName) {
        return cachePublishersPerIMap.get(mapName);
    }

    @Override
    public Map<String, PublisherRegistry> getAll() {
        return Collections.unmodifiableMap(cachePublishersPerIMap);
    }

    @Override
    public PublisherRegistry remove(String id) {
        return cachePublishersPerIMap.remove(id);
    }

    private PublisherRegistry createPublisherRegistry(String mapName) {
        return new PublisherRegistry(context, mapName);
    }
}

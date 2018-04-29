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

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Used to register and hold {@link SubscriberRegistry} per {@link com.hazelcast.core.IMap IMap}.
 *
 * @see SubscriberRegistry
 */
public class MapSubscriberRegistry implements Registry<String, SubscriberRegistry> {

    private final ConstructorFunction<String, SubscriberRegistry> registryConstructorFunction =
            new ConstructorFunction<String, SubscriberRegistry>() {
                @Override
                public SubscriberRegistry createNew(String mapName) {
                    return createSubscriberRegistry(mapName);
                }
            };

    private final QueryCacheContext context;
    private final ConcurrentMap<String, SubscriberRegistry> cachePublishersPerIMap;

    public MapSubscriberRegistry(QueryCacheContext context) {
        this.context = context;
        this.cachePublishersPerIMap = new ConcurrentHashMap<String, SubscriberRegistry>();
    }

    @Override
    public SubscriberRegistry getOrCreate(String mapName) {
        return getOrPutIfAbsent(cachePublishersPerIMap, mapName, registryConstructorFunction);
    }

    @Override
    public SubscriberRegistry getOrNull(String mapName) {
        return cachePublishersPerIMap.get(mapName);
    }

    @Override
    public Map<String, SubscriberRegistry> getAll() {
        return Collections.unmodifiableMap(cachePublishersPerIMap);
    }

    @Override
    public SubscriberRegistry remove(String mapName) {
        return cachePublishersPerIMap.remove(mapName);
    }

    protected SubscriberRegistry createSubscriberRegistry(String mapName) {
        return new SubscriberRegistry(context, mapName);
    }

    protected QueryCacheContext getContext() {
        return context;
    }
}

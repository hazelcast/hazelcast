/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Node side implementation of {@link QueryCacheConfigurator}.
 *
 * @see QueryCacheConfigurator
 */
public class NodeQueryCacheConfigurator extends AbstractQueryCacheConfigurator {

    private final Config config;
    private final NodeEngine engine;

    public NodeQueryCacheConfigurator(NodeEngine engine, Config config, QueryCacheEventService eventService) {
        super(eventService);
        this.engine = engine;
        this.config = config;
    }

    @Override
    public QueryCacheConfig getOrCreateConfiguration(String mapName, String cacheName, String cacheId) {
        MapConfig mapConfig = config.getMapConfig(mapName);

        QueryCacheConfig queryCacheConfig = findQueryCacheConfigFromMapConfig(mapConfig, cacheName);
        ClassLoader loader = NamespaceUtil.getClassLoaderForNamespace(engine, mapConfig.getUserCodeNamespace());

        if (queryCacheConfig != null) {
            setPredicateImpl(queryCacheConfig, loader);
            setEntryListener(mapName, cacheId, queryCacheConfig, loader);
            return queryCacheConfig;
        }

        QueryCacheConfig newConfig = new QueryCacheConfig(cacheName);
        mapConfig.getQueryCacheConfigs().add(newConfig);
        return newConfig;
    }

    @Override
    public QueryCacheConfig getOrNull(String mapName, String cacheName, String cacheId) {
        MapConfig mapConfig = config.getMapConfigOrNull(mapName);
        if (mapConfig == null) {
            return null;
        }
        ClassLoader loader = NamespaceUtil.getClassLoaderForNamespace(engine, mapConfig.getUserCodeNamespace());

        QueryCacheConfig queryCacheConfig = findQueryCacheConfigFromMapConfig(mapConfig, cacheName);
        if (queryCacheConfig != null) {
            setPredicateImpl(queryCacheConfig, loader);
            setEntryListener(mapName, cacheId, queryCacheConfig, loader);
            return queryCacheConfig;
        }

        return queryCacheConfig;
    }

    private QueryCacheConfig findQueryCacheConfigFromMapConfig(MapConfig mapConfig, String cacheName) {
        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        Map<String, QueryCacheConfig> allQueryCacheConfigs = new HashMap<>(queryCacheConfigs.size());
        for (QueryCacheConfig queryCacheConfig : queryCacheConfigs) {
            allQueryCacheConfigs.put(queryCacheConfig.getName(), queryCacheConfig);
        }

        return ConfigUtils.lookupByPattern(config.getConfigPatternMatcher(), allQueryCacheConfigs, cacheName);
    }

    @Override
    public void removeConfiguration(String mapName, String cacheName) {
        MapConfig mapConfig = config.getMapConfig(mapName);
        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        if (queryCacheConfigs == null || queryCacheConfigs.isEmpty()) {
            return;
        }
        queryCacheConfigs.removeIf(config -> config.getName().equals(cacheName));
    }
}

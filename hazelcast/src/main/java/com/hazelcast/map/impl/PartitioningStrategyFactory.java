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

package com.hazelcast.map.impl;

import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for {@link PartitioningStrategy} instances.
 */
public final class PartitioningStrategyFactory {
    // not private for tests
    final ConcurrentHashMap<String, PartitioningStrategy> cache = new ConcurrentHashMap<>();

    // this is set to the current NodeEngine.getConfigClassLoader
    private final ClassLoader configClassLoader;

    /**
     * Construct a new PartitioningStrategyFactory
     *
     * @param configClassLoader the current {@code NodeEngine}'s {@code configClassLoader}.
     */
    public PartitioningStrategyFactory(ClassLoader configClassLoader) {
        this.configClassLoader = configClassLoader;
    }

    /**
     * Obtain a {@link PartitioningStrategy} for the given {@code NodeEngine} and {@code mapName}. This method
     * first attempts locating a {@link PartitioningStrategy} in {code config.getPartitioningStrategy()}. If this is {@code null},
     * then looks up its internal cache of partitioning strategies; if one has already been created for the given
     * {@code mapName}, it is returned, otherwise it is instantiated, cached and returned.
     *
     * @param mapName          Map for which this partitioning strategy is being created
     * @param config           The partitioning strategy configuration
     * @param attributeConfigs The partitioning attributes
     * @return
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    public PartitioningStrategy getPartitioningStrategy(
            String mapName,
            PartitioningStrategyConfig config,
            final List<PartitioningAttributeConfig> attributeConfigs
    ) {
        PartitioningStrategy strategy = null;
        if (attributeConfigs != null && !attributeConfigs.isEmpty()) {
            strategy = cache.containsKey(mapName)
                    ? cache.get(mapName)
                    : createAttributePartitionStrategy(attributeConfigs);
        } else if (config != null) {
            strategy = config.getPartitioningStrategy();
            if (strategy == null) {
                if (cache.containsKey(mapName)) {
                    strategy = cache.get(mapName);
                } else if (config.getPartitioningStrategyClass() != null) {
                    try {
                        strategy = ClassLoaderUtil.newInstance(configClassLoader, config.getPartitioningStrategyClass());
                        cache.put(mapName, strategy);
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                }
            }
        }
        return strategy;
    }

    private PartitioningStrategy createAttributePartitionStrategy(final List<PartitioningAttributeConfig> attributes) {
        final String[] args = attributes.stream()
                .map(PartitioningAttributeConfig::getAttributeName)
                .toArray(String[]::new);

        return new AttributePartitioningStrategy(args);
    }

    /**
     * Remove the cached {@code PartitioningStrategy} from the internal cache, if it exists.
     *
     * @param mapName name of the map whose partitioning strategy will be removed from internal cache
     */
    public void removePartitioningStrategyFromCache(String mapName) {
        cache.remove(mapName);
    }
}

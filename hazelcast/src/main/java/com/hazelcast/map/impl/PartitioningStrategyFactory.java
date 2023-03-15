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

import javax.annotation.Nullable;
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
     * Obtain a {@link PartitioningStrategy} for the given {@code mapName}. This method
     * first checks the `attributeConfigs` - if it is non-empty, the strategy is created based on that.
     * Then it checks `config.getPartitioningStrategy()`, and returns it, if it's not null.
     * Then it checks `config.getPartitioningStrategyClass()`, and uses that to create the strategy.
     * If none is present, null is returned.
     * The instantiated strategy instances are cached for the map name.
     *
     * @param mapName          Map for which this partitioning strategy is being created
     * @param config           The partitioning strategy configuration
     * @param attributeConfigs The partitioning attributes
     * @return
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    @Nullable
    public PartitioningStrategy getPartitioningStrategy(
            String mapName,
            PartitioningStrategyConfig config,
            final List<PartitioningAttributeConfig> attributeConfigs
    ) {
        if (attributeConfigs != null && !attributeConfigs.isEmpty()) {
            return cache.computeIfAbsent(mapName, k -> createAttributePartitionStrategy(attributeConfigs));
        }
        if (config != null && config.getPartitioningStrategy() != null) {
            return config.getPartitioningStrategy();
        }
        if (config != null && config.getPartitioningStrategyClass() != null) {
            PartitioningStrategy<?> strategy = cache.get(mapName);
            if (strategy != null) {
                return strategy;
            }
            try {
                // We don't use computeIfAbsent intentionally so that the map isn't blocked if the instantiation takes a
                // long time - it's user code
                strategy = ClassLoaderUtil.newInstance(configClassLoader, config.getPartitioningStrategyClass());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
            cache.putIfAbsent(mapName, strategy);
            return strategy;
        }
        return null;
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

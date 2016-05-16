/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for {@link PartitioningStrategy} instances.
 */
public final class PartitioningStrategyFactory {

    private static final ConcurrentHashMap<String, PartitioningStrategy> CACHE =
            new ConcurrentHashMap<String, PartitioningStrategy>();

    private PartitioningStrategyFactory() {
    }

    /**
     * Obtain a {@link PartitioningStrategy} for the given {@code NodeEngine} and {@code PartitioningStrategyConfig}. This method
     * first attempts locating a {@link PartitioningStrategy} in {code config.getPartitioningStrategy()}. If this is {@code null},
     * then looks up its internal cache of partitioning strategies; if one has already been created for the given
     * {@code mapName}, it is returned, otherwise it is instantiated, cached and returned.
     * @param nodeEngine    Hazelcast NodeEngine
     * @param mapName       Map for which this partitioning strategy is being created
     * @param config        the partitioning strategy configuration
     * @return
     */
    public static PartitioningStrategy getPartitioningStrategy(NodeEngine nodeEngine, String mapName,
                                                               PartitioningStrategyConfig config) {
            PartitioningStrategy strategy = null;
            if (config != null) {
                strategy = config.getPartitioningStrategy();
                if (strategy == null) {
                    if (CACHE.containsKey(mapName)) {
                        strategy = CACHE.get(mapName);
                    } else if (config.getPartitioningStrategyClass() != null) {
                        try {
                            strategy = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(),
                                    config.getPartitioningStrategyClass());
                            CACHE.put(mapName, strategy);
                        } catch (Exception e) {
                            throw ExceptionUtil.rethrow(e);
                        }
                    }
                }
            }
            return strategy;
    }

    /**
     * Remove the cached {@code PartitioningStrategy} from the internal cache, if it exists.
     * @param mapName name of the map whose partitioning strategy will be removed from internal cache
     */
    public static void removePartitioningStrategyFromCache(String mapName) {
        CACHE.remove(mapName);
    }
}

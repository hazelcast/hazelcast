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

package com.hazelcast.partition.strategy;

import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

/**
 * Factory for {@link PartitioningStrategy} instances.
 */
public final class PartitioningStrategyFactory {

    private PartitioningStrategyFactory() {
    }

    /**
     * Obtain a {@link PartitioningStrategy} for the given {@code NodeEngine} and {@code PartitioningStrategyConfig}.
     * @param nodeEngine
     * @param config
     * @return
     */
    public static PartitioningStrategy getPartitioningStrategy(NodeEngine nodeEngine, PartitioningStrategyConfig config) {
            PartitioningStrategy strategy = null;
            if (config != null) {
                strategy = config.getPartitioningStrategy();
                if (strategy == null && config.getPartitioningStrategyClass() != null) {
                    try {
                        strategy = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(),
                                config.getPartitioningStrategyClass());
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                }
            }
            return strategy;
    }
}

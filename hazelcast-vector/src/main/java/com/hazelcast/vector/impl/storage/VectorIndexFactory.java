/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.storage;

import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.concurrent.ForkJoinPool;

import static com.hazelcast.internal.util.ThreadUtil.createWorkerPoolName;
import static com.hazelcast.spi.properties.ClusterProperty.INDEX_ISOLATED_EXECUTOR_MAX_PARALLELISM;
import static com.hazelcast.spi.properties.ClusterProperty.USE_INDEX_ISOLATED_EXECUTOR;

public class VectorIndexFactory {

    private final ForkJoinPool parallelExecutor;

    public VectorIndexFactory(NodeEngine nodeEngine) {
        this.parallelExecutor = createParallelExecutor(nodeEngine);
    }

    public AbstractVectorIndex create(VectorIndexConfig indexConfig) {
        if (indexConfig.isUseDeduplication()) {
            return new VectorIndexMultipleKeys(
                indexConfig.getName(),
                indexConfig.getMetric(),
                indexConfig.getMaxDegree(),
                indexConfig.getEfConstruction(),
                indexConfig.getDimension(),
                parallelExecutor
            );
        }
        return new VectorIndexSingleKey(
            indexConfig.getName(),
            indexConfig.getMetric(),
            indexConfig.getMaxDegree(),
            indexConfig.getEfConstruction(),
            indexConfig.getDimension(),
            parallelExecutor
        );
    }

    private ForkJoinPool createParallelExecutor(NodeEngine nodeEngine) {
        boolean enabled = nodeEngine.getProperties().getBoolean(USE_INDEX_ISOLATED_EXECUTOR);

        if (enabled) {
            return NamedForkJoinPoolProvider.create(
                createWorkerPoolName(nodeEngine.getHazelcastInstance().getName(), "vector-index"),
                resolveParallelism(nodeEngine)
            );
        }
        return ForkJoinPool.commonPool();
    }

    private static int resolveParallelism(NodeEngine nodeEngine) {
        int configured = nodeEngine.getProperties().getInteger(INDEX_ISOLATED_EXECUTOR_MAX_PARALLELISM);

        if (configured > 0) {
            return Math.min(configured, RuntimeAvailableProcessors.get() * 2);
        }

        return ForkJoinPool.getCommonPoolParallelism();
    }

    public void shutdown() {
        if (parallelExecutor != ForkJoinPool.commonPool()) {
            parallelExecutor.shutdown();
        }
    }
}

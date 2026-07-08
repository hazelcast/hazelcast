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

package com.hazelcast.vector.internal.impl.storage;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory utility for creating named {@link ForkJoinPool} instances.
 * This provider allows creation of ForkJoinPools with custom thread naming.
 */
public class NamedForkJoinPoolProvider {

    private NamedForkJoinPoolProvider() {
    }

    public static ForkJoinPool create(String poolName, int parallelism) {
       return new ForkJoinPool(
                parallelism,
                new IndexForkJoinWorkerThreadFactory(poolName),
                null,
                false
            );
    }

    private static final class IndexForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

        private final String poolName;
        private final AtomicInteger threadId = new AtomicInteger();

        private IndexForkJoinWorkerThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName(poolName + threadId.incrementAndGet());
            return worker;
        }
    }
}

/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.executor;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelExecutorService {
    private final ExecutorService executorService;

    public ParallelExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public ParallelExecutor newParallelExecutor(int concurrencyLevel) {
        if (concurrencyLevel > 0 && concurrencyLevel < Integer.MAX_VALUE) {
            return new ParallelExecutorImpl(concurrencyLevel);
        } else {
            return new FullyParallelExecutorImpl();
        }
    }

    class FullyParallelExecutorImpl implements ParallelExecutor {
        public void execute(Runnable runnable) {
            executorService.execute(runnable);
        }

        public void execute(Runnable runnable, int hash) {
            executorService.execute(runnable);
        }

        public void shutdown() {
        }

        public int getPoolSize() {
            return 0;
        }
    }

    class ParallelExecutorImpl implements ParallelExecutor {
        final ExecutionSegment[] executionSegments;
        final AtomicInteger offerIndex = new AtomicInteger();

        ParallelExecutorImpl(int concurrencyLevel) {
            this.executionSegments = new ExecutionSegment[concurrencyLevel];
            for (int i = 0; i < concurrencyLevel; i++) {
                executionSegments[i] = new ExecutionSegment();
            }
        }

        public void execute(Runnable runnable) {
            int index = offerIndex.incrementAndGet();
            ExecutionSegment segment = executionSegments[index % executionSegments.length];
            segment.offer(runnable);
            if (index >= 1000000) {
                offerIndex.set(0);
            }
        }

        public void execute(Runnable runnable, int hash) {
            ExecutionSegment segment = executionSegments[hash % executionSegments.length];
            segment.offer(runnable);
        }

        public void shutdown() {
            for (ExecutionSegment executionSegment : executionSegments) {
                executionSegment.shutdown();
            }
        }

        public int getPoolSize() {
            int size = 0;
            for (ExecutionSegment executionSegment : executionSegments) {
                size += executionSegment.size();
            }
            return size;
        }

        class ExecutionSegment implements Runnable {
            final ConcurrentLinkedQueue<Runnable> q = new ConcurrentLinkedQueue<Runnable>();
            final AtomicInteger size = new AtomicInteger();

            public void offer(Runnable e) {
                q.offer(e);
                if (size.incrementAndGet() == 1) {
                    executorService.execute(ExecutionSegment.this);
                }
            }

            public void run() {
                Runnable r = q.poll();
                while (r != null) {
                    r.run();
                    size.decrementAndGet();
                    r = q.poll();
                }
            }

            public void shutdown() {
                Runnable r = q.poll();
                while (r != null) {
                    size.decrementAndGet();
                    r = q.poll();
                }
            }

            public int size() {
                return size.get();
            }
        }
    }
}

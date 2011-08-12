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

import com.hazelcast.logging.ILogger;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class ParallelExecutorService {
    private final ExecutorService executorService;
    private final List<ParallelExecutor> lsParallelExecutors = new CopyOnWriteArrayList<ParallelExecutor>();
    private final ILogger logger;

    public ParallelExecutorService(ILogger logger, ExecutorService executorService) {
        this.executorService = executorService;
        this.logger = logger;
    }

    public void shutdown() {
        for (ParallelExecutor parallelExecutor : lsParallelExecutors) {
            parallelExecutor.shutdown();
        }
        lsParallelExecutors.clear();
    }

    public ParallelExecutor newBlockingParallelExecutor(int concurrencyLevel, int capacity) {
        ParallelExecutor p = new BlockingParallelExecutorImpl(concurrencyLevel, capacity);
        lsParallelExecutors.add(p);
        return p;
    }

    public ParallelExecutor newParallelExecutor(int concurrencyLevel) {
        ParallelExecutor parallelExecutor = null;
        if (concurrencyLevel > 0 && concurrencyLevel < Integer.MAX_VALUE) {
            parallelExecutor = new ParallelExecutorImpl(concurrencyLevel);
        } else {
            parallelExecutor = new FullyParallelExecutorImpl();
        }
        lsParallelExecutors.add(parallelExecutor);
        return parallelExecutor;
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

        public int getActiveCount() {
            return 0;
        }
    }

    class BlockingParallelExecutorImpl extends ParallelExecutorImpl {
        private final BlockingQueue<Object> q;

        BlockingParallelExecutorImpl(int concurrencyLevel, int capacity) {
            super(concurrencyLevel);
            q = new ArrayBlockingQueue<Object>(capacity);
        }

        @Override
        protected void onOffer() {
            try {
                q.put(Boolean.TRUE);
            } catch (InterruptedException e) {
            }
        }

        @Override
        protected void afterRun() {
            q.poll();
        }
    }

    class ParallelExecutorImpl implements ParallelExecutor {
        final ExecutionSegment[] executionSegments;
        final AtomicInteger offerIndex = new AtomicInteger();
        final AtomicInteger activeCount = new AtomicInteger();
        final AtomicLong waitingExecutions = new AtomicLong();

        ParallelExecutorImpl(int concurrencyLevel) {
            this.executionSegments = new ExecutionSegment[concurrencyLevel];
            for (int i = 0; i < concurrencyLevel; i++) {
                executionSegments[i] = new ExecutionSegment(i);
            }
        }

        public void execute(Runnable runnable) {
            int hash = offerIndex.incrementAndGet();
            execute(runnable, hash);
        }

        public void execute(Runnable runnable, int hash) {
            int index = (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % executionSegments.length;
            ExecutionSegment segment = executionSegments[index];
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

        public int getActiveCount() {
            return activeCount.get();
        }

        public long getQueueSize() {
            return waitingExecutions.get();
        }

        protected void onOffer() {
        }

        protected void beforeRun() {
        }

        protected void afterRun() {
        }

        class ExecutionSegment implements Runnable {
            final LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
            final AtomicInteger size = new AtomicInteger();
            final int segmentIndex;
            volatile boolean executing = false;

            ExecutionSegment(int segmentIndex) {
                this.segmentIndex = segmentIndex;
            }

            public void offer(Runnable e) {
                waitingExecutions.incrementAndGet();
                q.offer(e);
                onOffer();
                // ordering of executing and size is
                // very important for possible race issue.
                // these are both volatile. order is guaranteed.
                if (!executing && size.incrementAndGet() == 1) {
                    executorService.execute(ExecutionSegment.this);
                }
            }

            public void run() {
                executing = true;
                activeCount.incrementAndGet();
                Runnable r = null;
                try {
                    r = q.poll();
                    while (r != null) {
                        try {
                            beforeRun();
                            r.run();
                            afterRun();
                            waitingExecutions.decrementAndGet();
                        } catch (Throwable e) {
                            logger.log(Level.WARNING, e.getMessage(), e);
                        }
                        size.decrementAndGet();
                        r = q.poll(5, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    activeCount.decrementAndGet();
                    executing = false;
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

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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Tasklet;
import com.hazelcast.jet2.TaskletResult;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ExecutionService {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));
    private final Worker[] workers;
    private final Thread[] threads;

    public ExecutionService(JetEngineConfig cfg) {
        this.workers = new Worker[cfg.parallelism()];
        this.threads = new Thread[cfg.parallelism()];
    }

    public Future<Void> execute(List<Tasklet> tasklets) {
        final List<Tasklet>[] perWorkerTasklets = new List[workers.length];
        Arrays.setAll(perWorkerTasklets, i -> new ArrayList());
        int i = 0;
        for (Tasklet t : tasklets) {
            perWorkerTasklets[i++ % perWorkerTasklets.length].add(t);
        }
        final CountDownLatch completionLatch = new CountDownLatch(workers.length);
        final AtomicReference<Throwable> trouble = new AtomicReference<>();
        Arrays.setAll(workers, j -> new Worker(completionLatch, trouble, perWorkerTasklets[j]));
        return new JobFuture(completionLatch, trouble);
    }

    private class Worker implements Runnable {
        private final List<Tasklet> tasklets;
        private final CountDownLatch completionLatch;
        private final AtomicReference<Throwable> trouble;
        private long idleCount;

        public Worker(CountDownLatch completionLatch, AtomicReference<Throwable> trouble, List<Tasklet> tasklets) {
            this.completionLatch = completionLatch;
            this.trouble = trouble;
            this.tasklets = new CopyOnWriteArrayList<>(tasklets);
        }

        @Override
        public void run() {
            try {
                boolean madeProgress = false;
                for (Iterator<Tasklet> it = tasklets.iterator(); it.hasNext(); ) {
                    final Tasklet t = it.next();
                    final TaskletResult result = t.call();
                    switch (result) {
                        case DONE:
                            it.remove();
                            stealWork();
                            break;
                        case MADE_PROGRESS:
                            madeProgress = true;
                            break;
                        case NO_PROGRESS:
                            break;
                    }
                }
                if (tasklets.isEmpty()) {
                    return;
                }
                if (madeProgress) {
                    idleCount = 0;
                } else {
                    IDLER.idle(++idleCount);
                }
            } catch (Throwable e) {
                trouble.compareAndSet(null, e);
            } finally {
                completionLatch.countDown();
            }
        }

        private void stealWork() {
            while (true) {
                List<Tasklet> toStealFrom = tasklets;
                for (Worker w : workers) {
                    if (w.tasklets.size() > toStealFrom.size()) {
                        toStealFrom = w.tasklets;
                    }
                }
                if (toStealFrom == tasklets) {
                    return;
                }
                final Iterator<Tasklet> it = toStealFrom.iterator();
                if (!it.hasNext()) {
                    continue;
                }
                final Tasklet stolenTask = it.next();
                if (toStealFrom.remove(stolenTask)) {
                    tasklets.add(stolenTask);
                    return;
                }
            }
        }
    }


    static class JobFuture implements Future<Void> {

        private final CountDownLatch completionLatch;
        private final AtomicReference<Throwable> trouble;

        JobFuture(CountDownLatch completionLatch, AtomicReference<Throwable> trouble) {
            this.completionLatch = completionLatch;
            this.trouble = trouble;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return completionLatch.getCount() == 0;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            try {
                return get(Long.MAX_VALUE, SECONDS);
            } catch (TimeoutException e) {
                throw new Error("Impossible timeout");
            }
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (!completionLatch.await(timeout, unit)) {
                throw new TimeoutException("Jet Execution Service");
            }
            final Throwable t = trouble.get();
            if (t != null) {
                throw new ExecutionException(t);
            }
            return null;
        }
    }
}

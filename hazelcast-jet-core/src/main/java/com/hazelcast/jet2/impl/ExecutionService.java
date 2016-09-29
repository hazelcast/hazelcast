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
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
        final List<TaskletTracker>[] trackers = new List[workers.length];
        Arrays.setAll(trackers, i -> new ArrayList());
        int i = 0;
        final CountDownLatch completionLatch = new CountDownLatch(workers.length);
        final AtomicReference<Throwable> trouble = new AtomicReference<>();
        for (Tasklet t : tasklets) {
            trackers[i++ % trackers.length].add(new TaskletTracker(t, completionLatch, trouble));
        }
        Arrays.setAll(workers, j -> new Worker(trackers[j]));
        return new JobFuture(completionLatch, trouble);
    }

    private class Worker implements Runnable {
        private final List<TaskletTracker> trackers;
        private long idleCount;

        public Worker(List<TaskletTracker> trackers) {
            this.trackers = new CopyOnWriteArrayList<>(trackers);
        }

        @Override
        public void run() {
            boolean madeProgress = false;
            for (Iterator<TaskletTracker> it = trackers.iterator(); it.hasNext(); ) {
                final TaskletTracker t = it.next();
                if (t.trouble.get() != null) {
                    t.completionLatch.countDown();
                    it.remove();
                    stealWork();
                    continue;
                }
                final Worker stealingWorker = t.stealingWorker.get();
                if (stealingWorker != null) {
                    t.stealingWorker.set(null);
                    it.remove();
                    stealingWorker.trackers.add(t);
                }
                try {
                    final TaskletResult result = t.tasklet.call();
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
                } catch (Throwable e) {
                    t.trouble.compareAndSet(null, e);
                    it.remove();
                    stealWork();
                }
            }
            if (madeProgress) {
                idleCount = 0;
            } else {
                IDLER.idle(++idleCount);
            }
        }

        private void stealWork() {
            while (true) {
                // start with own tasklet list, try to find a longer one
                List<TaskletTracker> toStealFrom = trackers;
                for (Worker w : workers) {
                    if (w.trackers.size() > toStealFrom.size()) {
                        toStealFrom = w.trackers;
                    }
                }
                // if we couldn't find a longer one, there's nothing to steal
                if (toStealFrom == trackers) {
                    return;
                }
                for (TaskletTracker t : toStealFrom) {
                    if (t.stealingWorker.compareAndSet(null, this)) {
                        return;
                    }
                }
            }
        }
    }

    static final class TaskletTracker {
        final Tasklet tasklet;
        final CountDownLatch completionLatch;
        final AtomicReference<Throwable> trouble;
        final AtomicReference<Worker> stealingWorker = new AtomicReference<>();

        public TaskletTracker(Tasklet tasklet, CountDownLatch completionLatch, AtomicReference<Throwable> trouble) {
            this.completionLatch = completionLatch;
            this.tasklet = tasklet;
            this.trouble = trouble;
        }
    }
}

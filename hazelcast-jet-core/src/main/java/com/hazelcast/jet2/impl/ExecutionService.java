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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.partitioningBy;

class ExecutionService {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));
    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final NonBlockingWorker[] workers;
    private final Thread[] threads;

    public ExecutionService(JetEngineConfig cfg) {
        this.workers = new NonBlockingWorker[cfg.getParallelism()];
        this.threads = new Thread[cfg.getParallelism()];
    }

    public Future<Void> execute(List<Tasklet> tasklets) {
        final CountDownLatch completionLatch = new CountDownLatch(tasklets.size());
        final JobFuture jobFuture = new JobFuture(completionLatch);
        final Map<Boolean, List<Tasklet>> byBlocking = tasklets.stream().collect(partitioningBy(Tasklet::isBlocking));
        submitBlockingTasklets(completionLatch, jobFuture, byBlocking.get(true));
        submitNonblockingTasklets(completionLatch, jobFuture, byBlocking.get(false));
        return jobFuture;
    }

    private void submitBlockingTasklets(CountDownLatch completionLatch, JobFuture jobFuture, List<Tasklet> tasklets) {
        for (Tasklet t : tasklets) {
            blockingTaskletExecutor.execute(new BlockingWorker(new TaskletTracker(t, completionLatch, jobFuture)));
        }
    }

    private void submitNonblockingTasklets(CountDownLatch completionLatch, JobFuture jobFuture, List<Tasklet> tasklets) {
        ensureThreadsStarted();
        final List<TaskletTracker>[] trackersByThread = new List[workers.length];
        Arrays.setAll(trackersByThread, i -> new ArrayList());
        int i = 0;
        for (Tasklet t : tasklets) {
            if (initPropagatingFailure(t, jobFuture, completionLatch)) {
                trackersByThread[i++ % trackersByThread.length].add(new TaskletTracker(t, completionLatch, jobFuture));
            }
        }
        for (i = 0; i < trackersByThread.length; i++) {
            workers[i].trackers.addAll(trackersByThread[i]);
        }
        Arrays.stream(threads).forEach(LockSupport::unpark);
    }

    private void ensureThreadsStarted() {
        if (workers[0] != null) {
            return;
        }
        Arrays.setAll(workers, i -> new NonBlockingWorker(workers));
        Arrays.setAll(threads, i -> new Thread(workers[i], "Non-blocking tasklet thread #" + i));
        Arrays.stream(threads).forEach(Thread::start);
    }

    private static boolean initPropagatingFailure(Tasklet t, JobFuture jobFuture, CountDownLatch completionLatch) {
        try {
            t.init();
            return true;
        } catch (Throwable e) {
            jobFuture.setTrouble(e);
            completionLatch.countDown();
            return false;
        }
    }

    private static final class BlockingWorker implements Runnable {
        private final TaskletTracker tracker;

        private BlockingWorker(TaskletTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        @SuppressWarnings("checkstyle:innerassignment")
        public void run() {
            final Tasklet t = tracker.tasklet;
            try {
                t.init();
                long idleCount = 0;
                for (ProgressState result; !(result = t.call()).isDone() && !tracker.troubleSetter.hasTrouble(); ) {
                    if (result.isMadeProgress()) {
                        idleCount = 0;
                    } else {
                        IDLER.idle(++idleCount);
                    }
                }
            } catch (Throwable e) {
                tracker.troubleSetter.setTrouble(e);
            } finally {
                tracker.completionLatch.countDown();
            }
        }
    }

    private static class NonBlockingWorker implements Runnable {
        private final List<TaskletTracker> trackers;
        private final NonBlockingWorker[] colleagues;

        NonBlockingWorker(NonBlockingWorker[] colleagues) {
            this.colleagues = colleagues;
            this.trackers = new CopyOnWriteArrayList<>();
        }

        @Override
        public void run() {
            long idleCount = 0;
            while (true) {
                boolean madeProgress = false;
                for (TaskletTracker t : trackers) {
                    final NonBlockingWorker stealingWorker = t.stealingWorker.get();
                    if (stealingWorker != null) {
                        t.stealingWorker.set(null);
                        trackers.remove(t);
                        stealingWorker.trackers.add(t);
                        continue;
                    }
                    try {
                        final ProgressState result = t.tasklet.call();
                        if (result.isDone()) {
                            dismissTasklet(t);
                        } else {
                            madeProgress |= result.isMadeProgress();
                        }
                    } catch (Throwable e) {
                        t.troubleSetter.setTrouble(e);
                    }
                    if (t.troubleSetter.hasTrouble()) {
                        dismissTasklet(t);
                    }
                }
                if (madeProgress) {
                    idleCount = 0;
                } else {
                    IDLER.idle(++idleCount);
                }
            }
        }

        private void dismissTasklet(TaskletTracker t) {
            t.completionLatch.countDown();
            trackers.remove(t);
            stealWork();
        }

        private void stealWork() {
            while (true) {
                // start with own tasklet list, try to find a longer one
                List<TaskletTracker> toStealFrom = trackers;
                for (NonBlockingWorker w : colleagues) {
                    if (w.trackers.size() > toStealFrom.size()) {
                        toStealFrom = w.trackers;
                    }
                }
                // if we couldn't find a list longer by more than one, there's nothing to steal
                if (toStealFrom.size() <= trackers.size() + 1) {
                    return;
                }
                // now we must find a task on this list which isn't already scheduled for moving
                for (TaskletTracker t : toStealFrom) {
                    if (t.stealingWorker.compareAndSet(null, this)) {
                        return;
                    }
                }
            }
        }
    }

    private static final class TaskletTracker {
        final Tasklet tasklet;
        final TroubleSetter troubleSetter;
        final CountDownLatch completionLatch;
        final AtomicReference<NonBlockingWorker> stealingWorker = new AtomicReference<>();

        TaskletTracker(Tasklet tasklet, CountDownLatch completionLatch, TroubleSetter troubleSetter) {
            this.completionLatch = completionLatch;
            this.tasklet = tasklet;
            this.troubleSetter = troubleSetter;
        }
    }

    private static final class BlockingTaskThreadFactory implements ThreadFactory {
        private int seq;

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Blocking tasklet thread #" + seq++);
        }
    }
}

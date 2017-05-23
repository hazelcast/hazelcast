/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class ExecutionService {

    static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));

    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final CooperativeWorker[] cooperativeWorkers;
    private final Thread[] cooperativeThreadPool;
    private final String hzInstanceName;
    private final ILogger logger;
    private final AtomicInteger cooperativeThreadIndex = new AtomicInteger();

    private volatile boolean isShutdown;

    public ExecutionService(HazelcastInstance hz, int threadCount) {
        this.hzInstanceName = hz.getName();
        this.cooperativeWorkers = new CooperativeWorker[threadCount];
        this.cooperativeThreadPool = new Thread[threadCount];
        this.logger = hz.getLoggingService().getLogger(ExecutionService.class);
    }

    /**
     * @return instance of {@code java.util.concurrent.CompletableFuture}
     */
    public CompletionStage<Void> execute(
            @Nonnull List<? extends Tasklet> tasklets,
            @Nonnull Consumer<CompletionStage<Void>> doneCallback,
            @Nonnull ClassLoader jobClassLoader
    ) {
        ensureStillRunning();
        final JobFuture jobFuture = new JobFuture(tasklets.size(), doneCallback);
        try {
            final Map<Boolean, List<Tasklet>> byCooperation =
                    tasklets.stream().collect(partitioningBy(Tasklet::isCooperative));
            submitCooperativeTasklets(jobFuture, jobClassLoader, byCooperation.get(true));
            submitBlockingTasklets(jobFuture, jobClassLoader, byCooperation.get(false));
        } catch (Throwable t) {
            jobFuture.completeExceptionally(t);
            doneCallback.accept(jobFuture);
        }
        return jobFuture;
    }

    public void shutdown() {
        isShutdown = true;
        blockingTaskletExecutor.shutdownNow();
    }

    private void ensureStillRunning() {
        if (isShutdown) {
            throw new IllegalStateException("Execution service was already ordered to shut down");
        }
    }

    private void submitBlockingTasklets(JobFuture jobFuture, ClassLoader jobClassLoader, List<Tasklet> tasklets) {
        jobFuture.blockingFutures = tasklets
                .stream()
                .map(t -> new BlockingWorker(new TaskletTracker(t, jobFuture, jobClassLoader)))
                .map(blockingTaskletExecutor::submit)
                .collect(toList());
    }

    private void submitCooperativeTasklets(JobFuture jobFuture, ClassLoader jobClassLoader, List<Tasklet> tasklets) {
        ensureThreadsStarted();
        final List<TaskletTracker>[] trackersByThread = new List[cooperativeWorkers.length];
        Arrays.setAll(trackersByThread, i -> new ArrayList());
        for (Tasklet t : tasklets) {
            t.init(jobFuture);
            trackersByThread[Math.floorMod(cooperativeThreadIndex.getAndIncrement(), trackersByThread.length)]
                    .add(new TaskletTracker(t, jobFuture, jobClassLoader));
        }
        for (int i = 0; i < trackersByThread.length; i++) {
            cooperativeWorkers[i].trackers.addAll(trackersByThread[i]);
        }
        Arrays.stream(cooperativeThreadPool).forEach(LockSupport::unpark);
    }

    private synchronized void ensureThreadsStarted() {
        if (cooperativeWorkers[0] != null) {
            return;
        }
        Arrays.setAll(cooperativeWorkers, i -> new CooperativeWorker(cooperativeWorkers));
        Arrays.setAll(cooperativeThreadPool, i -> new Thread(cooperativeWorkers[i],
                String.format("hz.%s.jet.cooperative.thread-%d", hzInstanceName, i)));
        Arrays.stream(cooperativeThreadPool).forEach(Thread::start);
    }

    private String trackersToString() {
        return Arrays.stream(cooperativeWorkers)
                     .flatMap(w -> w.trackers.stream())
                     .map(Object::toString)
                     .sorted()
                     .collect(joining("\n"))
                + "\n-----------------";
    }

    private final class BlockingWorker implements Runnable {
        private final TaskletTracker tracker;

        private BlockingWorker(TaskletTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public void run() {
            final ClassLoader clBackup = currentThread().getContextClassLoader();
            final Tasklet t = tracker.tasklet;
            currentThread().setContextClassLoader(tracker.jobClassLoader);
            try {
                t.init(tracker.jobFuture);
                long idleCount = 0;
                for (ProgressState result;
                     !(result = t.call()).isDone() && !tracker.jobFuture.isDone() && !isShutdown;
                 ) {
                    if (result.isMadeProgress()) {
                        idleCount = 0;
                    } else {
                        IDLER.idle(++idleCount);
                    }
                }
            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                tracker.jobFuture.completeExceptionally(new JetException("Exception in " + t + ": " + e, e));
            } finally {
                currentThread().setContextClassLoader(clBackup);
                tracker.jobFuture.taskletDone();
            }
        }
    }

    private final class CooperativeWorker implements Runnable {
        private final List<TaskletTracker> trackers;
        private final CooperativeWorker[] colleagues;

        CooperativeWorker(CooperativeWorker[] colleagues) {
            this.colleagues = colleagues;
            this.trackers = new CopyOnWriteArrayList<>();
        }

        @Override
        public void run() {
            final Thread thread = currentThread();
            final ClassLoader clBackup = thread.getContextClassLoader();
            long idleCount = 0;
            while (!isShutdown) {
                boolean madeProgress = false;
                for (TaskletTracker t : trackers) {
                    final CooperativeWorker stealingWorker = t.stealingWorker.get();
                    if (stealingWorker != null) {
                        t.stealingWorker.set(null);
                        trackers.remove(t);
                        stealingWorker.trackers.add(t);
                        continue;
                    }
                    try {
                        thread.setContextClassLoader(t.jobClassLoader);
                        final ProgressState result = t.tasklet.call();
                        if (result.isDone()) {
                            dismissTasklet(t);
                        } else {
                            madeProgress |= result.isMadeProgress();
                        }
                    } catch (Throwable e) {
                        logger.warning("Exception in " + t.tasklet, e);
                        t.jobFuture.completeExceptionally(new JetException("Exception in " + t.tasklet + ": " + e, e));
                    }
                    if (t.jobFuture.isCompletedExceptionally()) {
                        dismissTasklet(t);
                    }
                }
                if (madeProgress) {
                    idleCount = 0;
                } else {
                    thread.setContextClassLoader(clBackup);
                    IDLER.idle(++idleCount);
                }
            }
            // Best-effort attempt to release all tasklets. A tasklet can still be added
            // to a dead worker through work stealing.
            trackers.clear();
        }

        private void dismissTasklet(TaskletTracker t) {
            t.jobFuture.taskletDone();
            trackers.remove(t);
            stealWork();
        }

        private void stealWork() {
            while (true) {
                // start with own tasklet list, try to find a longer one
                List<TaskletTracker> toStealFrom = trackers;
                for (CooperativeWorker w : colleagues) {
                    if (w.trackers.size() > toStealFrom.size()) {
                        toStealFrom = w.trackers;
                    }
                }
                // if we couldn't find a list longer by at least two, there's nothing to steal
                if (toStealFrom.size() < trackers.size() + 2) {
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
        final JobFuture jobFuture;
        final ClassLoader jobClassLoader;
        final AtomicReference<CooperativeWorker> stealingWorker = new AtomicReference<>();

        TaskletTracker(Tasklet tasklet, JobFuture jobFuture, ClassLoader jobClassLoader) {
            this.tasklet = tasklet;
            this.jobFuture = jobFuture;
            this.jobClassLoader = jobClassLoader;
        }

        @Override
        public String toString() {
            return "Tracking " + tasklet;
        }
    }

    private final class BlockingTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            return new Thread(r,
                    String.format("hz.%s.jet.blocking.thread-%d", hzInstanceName, seq.getAndIncrement()));
        }
    }

    private static final class JobFuture extends CompletableFuture<Void> {

        private final AtomicInteger completionLatch;
        private final Consumer<CompletionStage<Void>> doneCallback;
        private List<Future> blockingFutures;

        JobFuture(int taskletCount, Consumer<CompletionStage<Void>> doneCallback) {
            this.doneCallback = doneCallback;
            this.completionLatch = new AtomicInteger(taskletCount);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                blockingFutures.forEach(f -> f.cancel(true)); // CompletableFuture.cancel ignores the flag
            }
            return cancelled;
        }

        @SuppressFBWarnings(value = "NP_NONNULL_PARAM_VIOLATION", justification = "CompletableFuture<Void>")
        private void taskletDone() {
            if (completionLatch.decrementAndGet() == 0) {
                complete(null);
                if (doneCallback != null) {
                    doneCallback.accept(this);
                }
            }
        }
    }
}

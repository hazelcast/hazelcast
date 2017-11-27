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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class TaskletExecutionService {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));

    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final CooperativeWorker[] cooperativeWorkers;
    private final Thread[] cooperativeThreadPool;
    private final String hzInstanceName;
    private final ILogger logger;
    private final AtomicInteger cooperativeThreadIndex = new AtomicInteger();

    private volatile boolean isShutdown;

    public TaskletExecutionService(HazelcastInstance hz, int threadCount) {
        this.hzInstanceName = hz.getName();
        this.cooperativeWorkers = new CooperativeWorker[threadCount];
        this.cooperativeThreadPool = new Thread[threadCount];
        this.logger = hz.getLoggingService().getLogger(TaskletExecutionService.class);
    }

    /**
     * Submits the tasklets for execution and returns a future which is completed only
     * when execution of all the tasklets has completed. If an exception occurred during
     * execution or execution was cancelled then the future will be completed exceptionally
     * but only after all tasklets are finished executing. The returned future does not
     * support cancellation, instead the supplied {@code cancellationFuture} should be used.
     *
     * @param tasklets        tasklets to run
     * @param cancellationFuture A future when cancelled will cancel the execution of the tasklets
     * @param jobClassLoader  classloader to use when running the tasklets
     */
    CompletableFuture<Void> beginExecute(
            @Nonnull List<? extends Tasklet> tasklets,
            @Nonnull CompletableFuture<Void> cancellationFuture,
            @Nonnull ClassLoader jobClassLoader
    ) {
        ensureStillRunning();
        final ExecutionTracker executionTracker = new ExecutionTracker(tasklets.size(), cancellationFuture);
        try {
            final Map<Boolean, List<Tasklet>> byCooperation =
                    tasklets.stream().collect(partitioningBy(Tasklet::isCooperative));
            submitCooperativeTasklets(executionTracker, jobClassLoader, byCooperation.get(true));
            submitBlockingTasklets(executionTracker, jobClassLoader, byCooperation.get(false));
        } catch (Throwable t) {
            executionTracker.future.internalCompleteExceptionally(t);
        }
        return executionTracker.future;
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

    private void submitBlockingTasklets(ExecutionTracker executionTracker, ClassLoader jobClassLoader,
                                        List<Tasklet> tasklets) {
        CountDownLatch startedLatch = new CountDownLatch(tasklets.size());
        executionTracker.blockingFutures = tasklets
                .stream()
                .map(t -> new BlockingWorker(new TaskletTracker(t, executionTracker, jobClassLoader), startedLatch))
                .map(blockingTaskletExecutor::submit)
                .collect(toList());

        // do not return from this method until all workers have started. Otherwise on
        // cancellation there is a race that the worker might not be started by the executor yet.
        // This results the taskletDone() method never being called for a worker.
        uncheckRun(startedLatch::await);
    }

    private void submitCooperativeTasklets(
            ExecutionTracker executionTracker, ClassLoader jobClassLoader, List<Tasklet> tasklets
    ) {
        ensureThreadsStarted();
        final List<TaskletTracker>[] trackersByThread = new List[cooperativeWorkers.length];
        Arrays.setAll(trackersByThread, i -> new ArrayList());
        for (Tasklet t : tasklets) {
            t.init();
            trackersByThread[cooperativeThreadIndex.getAndUpdate(i -> (i + 1) % trackersByThread.length)]
                    .add(new TaskletTracker(t, executionTracker, jobClassLoader));
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
        private final CountDownLatch startedLatch;

        private BlockingWorker(TaskletTracker tracker, CountDownLatch startedLatch) {
            this.tracker = tracker;
            this.startedLatch = startedLatch;
        }

        @Override
        public void run() {
            final ClassLoader clBackup = currentThread().getContextClassLoader();
            final Tasklet t = tracker.tasklet;
            currentThread().setContextClassLoader(tracker.jobClassLoader);
            try {
                startedLatch.countDown();
                t.init();
                long idleCount = 0;
                ProgressState result;
                do {
                    result = t.call();
                    if (result.isMadeProgress()) {
                        idleCount = 0;
                    } else {
                        IDLER.idle(++idleCount);
                    }
                } while (!result.isDone()
                        && !tracker.executionTracker.executionCompletedExceptionally()
                        && !isShutdown);
            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                tracker.executionTracker.exception(new JetException("Exception in " + t + ": " + e, e));
            } finally {
                currentThread().setContextClassLoader(clBackup);
                tracker.executionTracker.taskletDone();
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
                        t.executionTracker.exception(new JetException("Exception in " + t.tasklet + ": " + e, e));
                    }
                    if (t.executionTracker.executionCompletedExceptionally()) {
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
            trackers.forEach(t -> t.executionTracker.taskletDone());
            trackers.clear();
        }

        private void dismissTasklet(TaskletTracker t) {
            t.executionTracker.taskletDone();
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
        final ExecutionTracker executionTracker;
        final ClassLoader jobClassLoader;
        final AtomicReference<CooperativeWorker> stealingWorker = new AtomicReference<>();

        TaskletTracker(Tasklet tasklet, ExecutionTracker executionTracker, ClassLoader jobClassLoader) {
            this.tasklet = tasklet;
            this.executionTracker = executionTracker;
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


    /**
     * Internal utility class to track the overall state of tasklet execution.
     * There's one instance of this class per job.
     */
    private final class ExecutionTracker {

        final ExecutionFuture future = new ExecutionFuture();
        List<Future> blockingFutures;

        private final AtomicInteger completionLatch;
        private final AtomicReference<Throwable> executionException = new AtomicReference<>();

        ExecutionTracker(int taskletCount, CompletableFuture<Void> cancellationFuture) {
            this.completionLatch = new AtomicInteger(taskletCount);

            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                if (!(e instanceof CancellationException)) {
                    exception(new IllegalStateException("cancellationFuture was completed with something " +
                            "other than CancellationException: " + e, e));
                    return;
                }
                exception(e);
                blockingFutures.forEach(f -> f.cancel(true)); // CompletableFuture.cancel ignores the flag
            }));
        }

        void exception(Throwable t) {
            executionException.compareAndSet(null, t);
        }

        void taskletDone() {
            if (completionLatch.decrementAndGet() == 0) {
                Throwable ex = executionException.get();
                if (ex == null) {
                    future.internalComplete();
                } else {
                    future.internalCompleteExceptionally(ex);
                }
            }
        }

        boolean executionCompletedExceptionally() {
            return executionException.get() != null;
        }
    }

    /**
     * ExecutionFuture which prevents completion from outside
     */
    private static class ExecutionFuture extends CompletableFuture<Void> {
        @Override
        public boolean completeExceptionally(Throwable ex) {
            throw new UnsupportedOperationException("This future can't be completed by an outside caller");
        }

        @Override
        public boolean complete(Void value) {
            throw new UnsupportedOperationException("This future can't be completed by an outside caller");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("This future can't be cancelled by an outside caller");
        }

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        void internalComplete() {
            super.complete(null);
        }

        void internalCompleteExceptionally(Throwable ex) {
            super.completeExceptionally(ex);
        }
    }
}

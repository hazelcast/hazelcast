/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.exception.ShutdownInProgressException;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class TaskletExecutionService {

    private static final IdleStrategy IDLER_COOPERATIVE =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));
    private static final IdleStrategy IDLER_NON_COOPERATIVE =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(5));

    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final CooperativeWorker[] cooperativeWorkers;
    private final Thread[] cooperativeThreadPool;
    private final String hzInstanceName;
    private final ILogger logger;
    private final AtomicInteger cooperativeThreadIndex = new AtomicInteger();
    @Probe
    private final AtomicInteger blockingWorkerCount = new AtomicInteger();

    // tri-state boolean:
    // - null: not shut down
    // - FALSE: shut down forcefully: break the execution loop, don't wait for tasklets to finish
    // - TRUE: shut down gracefully: run normally, don't accept more tasklets
    private final AtomicReference<Boolean> gracefulShutdown = new AtomicReference<>(null);

    public TaskletExecutionService(NodeEngineImpl nodeEngine, int threadCount) {
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.cooperativeWorkers = new CooperativeWorker[threadCount];
        this.cooperativeThreadPool = new Thread[threadCount];
        this.logger = nodeEngine.getLoggingService().getLogger(TaskletExecutionService.class);

        nodeEngine.getMetricsRegistry().newProbeBuilder()
                       .withTag("module", "jet")
                       .scanAndRegister(this);

        Arrays.setAll(cooperativeWorkers, i -> new CooperativeWorker(cooperativeWorkers));
        Arrays.setAll(cooperativeThreadPool, i -> new Thread(cooperativeWorkers[i],
                String.format("hz.%s.jet.cooperative.thread-%d", hzInstanceName, i)));
        Arrays.stream(cooperativeThreadPool).forEach(Thread::start);
        for (int i = 0; i < cooperativeWorkers.length; i++) {
            nodeEngine.getMetricsRegistry().newProbeBuilder()
                           .withTag("module", "jet")
                           .withTag("cooperativeWorker", String.valueOf(i))
                           .scanAndRegister(cooperativeWorkers[i]);
        }
    }

    /**
     * Submits the tasklets for execution and returns a future which gets
     * completed when the execution of all the tasklets has completed. If an
     * exception occurrs or the execution gets cancelled, the future will be
     * completed exceptionally, but only after all the tasklets have finished
     * executing. The returned future does not support cancellation, instead
     * the supplied {@code cancellationFuture} should be used.
     *
     * @param tasklets            tasklets to run
     * @param cancellationFuture  future that, if cancelled, will cancel the execution of the tasklets
     * @param jobClassLoader      classloader to use when running the tasklets
     */
    CompletableFuture<Void> beginExecute(
            @Nonnull List<? extends Tasklet> tasklets,
            @Nonnull CompletableFuture<Void> cancellationFuture,
            @Nonnull ClassLoader jobClassLoader
    ) {
        if (gracefulShutdown.get() != null) {
            throw new ShutdownInProgressException();
        }
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

    public void shutdown(boolean graceful) {
        if (gracefulShutdown.compareAndSet(null, graceful)) {
            if (graceful) {
                blockingTaskletExecutor.shutdown();
            } else {
                blockingTaskletExecutor.shutdownNow();
            }
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

        // do not return from this method until all workers have started. Otherwise
        // on cancellation there is a race where the executor might not have started
        // the worker yet. This would results in taskletDone() never being called for
        // a worker.
        uncheckRun(startedLatch::await);
    }

    private void submitCooperativeTasklets(
            ExecutionTracker executionTracker, ClassLoader jobClassLoader, List<Tasklet> tasklets
    ) {
        @SuppressWarnings("unchecked")
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

    private String trackersToString() {
        return Arrays.stream(cooperativeWorkers)
                     .flatMap(w -> w.trackers.stream())
                     .map(Object::toString)
                     .sorted()
                     .collect(joining("\n"))
                + "\n-----------------";
    }

    /**
     * Blocks until all workers terminate (cooperative & blocking).
     */
    public void awaitWorkerTermination() {
        assert gracefulShutdown.get() != null : "Not shut down";
        try {
            while (!blockingTaskletExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                logger.warning("Blocking tasklet executor did not terminate in 1 minute");
            }
            for (Thread t : cooperativeThreadPool) {
                t.join();
            }
        } catch (InterruptedException e) {
            sneakyThrow(e);
        }
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
            final String oldName = currentThread().getName();
            currentThread().setContextClassLoader(tracker.jobClassLoader);

            // swap the thread name by replacing the ".thread-NN" part at the end
            try {
                currentThread().setName(oldName.replaceAll(".thread-[0-9]+$", quoteReplacement("." + tracker.tasklet)));
                assert !oldName.equals(currentThread().getName()) : "unexpected thread name pattern: " + oldName;
                blockingWorkerCount.incrementAndGet();

                startedLatch.countDown();
                t.init();
                long idleCount = 0;
                ProgressState result;
                do {
                    result = t.call();
                    if (result.isMadeProgress()) {
                        idleCount = 0;
                    } else {
                        IDLER_NON_COOPERATIVE.idle(++idleCount);
                    }
                } while (!result.isDone()
                        && !tracker.executionTracker.executionCompletedExceptionally()
                        && !Boolean.FALSE.equals(gracefulShutdown.get()));
            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                tracker.executionTracker.exception(new JetException("Exception in " + t + ": " + e, e));
            } finally {
                blockingWorkerCount.decrementAndGet();
                currentThread().setContextClassLoader(clBackup);
                currentThread().setName(oldName);
                tracker.executionTracker.taskletDone();
            }
        }
    }

    private final class CooperativeWorker implements Runnable {
        private static final int COOPERATIVE_LOGGING_THRESHOLD = 5;

        @Probe(name = "taskletCount")
        private final List<TaskletTracker> trackers;
        private final CooperativeWorker[] colleagues;
        @Probe
        private final AtomicLong iterationCount = new AtomicLong();

        CooperativeWorker(CooperativeWorker[] colleagues) {
            this.colleagues = colleagues;
            this.trackers = new CopyOnWriteArrayList<>();
        }

        @Override
        public void run() {
            final Thread thread = currentThread();
            long idleCount = 0;
            while (true) {
                Boolean gracefulShutdownLocal = gracefulShutdown.get();
                // exit condition
                if (gracefulShutdownLocal != null && (!gracefulShutdownLocal || trackers.isEmpty())) {
                    break;
                }
                boolean madeProgress = false;
                for (TaskletTracker t : trackers) {
                    long start = 0;
                    if (logger.isFinestEnabled()) {
                        start = System.nanoTime();
                    }
                    final CooperativeWorker stealingWorker = t.stealingWorker.get();
                    if (stealingWorker != null) {
                        t.stealingWorker.set(null);
                        trackers.remove(t);
                        stealingWorker.trackers.add(t);
                        logFinest(logger, "Tasklet %s was stolen from this worker", t.tasklet);
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

                    if (logger.isFinestEnabled()) {
                        long elapsedMs = NANOSECONDS.toMillis((System.nanoTime() - start));
                        if (elapsedMs > COOPERATIVE_LOGGING_THRESHOLD) {
                            logger.finest("Cooperative tasklet call of '" + t.tasklet + "' took more than "
                                    + COOPERATIVE_LOGGING_THRESHOLD + " ms: " + elapsedMs + "ms");
                        }
                    }
                }
                lazyIncrement(iterationCount);
                if (madeProgress) {
                    idleCount = 0;
                } else {
                    IDLER_COOPERATIVE.idle(++idleCount);
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

        final NonCompletableFuture future = new NonCompletableFuture();
        volatile List<Future> blockingFutures = emptyList();

        private final AtomicInteger completionLatch;
        private final AtomicReference<Throwable> executionException = new AtomicReference<>();

        ExecutionTracker(int taskletCount, CompletableFuture<Void> cancellationFuture) {
            this.completionLatch = new AtomicInteger(taskletCount);
            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                if (e == null) {
                    e = new IllegalStateException("cancellationFuture should be completed exceptionally");
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

}

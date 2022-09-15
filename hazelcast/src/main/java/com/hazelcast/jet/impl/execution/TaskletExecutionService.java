/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.sql.impl.ResultLimitReachedException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.executor.ExecutorType.CACHED;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.spi.properties.ClusterProperty.JET_IDLE_COOPERATIVE_MAX_MICROSECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.JET_IDLE_COOPERATIVE_MIN_MICROSECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.JET_IDLE_NONCOOPERATIVE_MAX_MICROSECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.JET_IDLE_NONCOOPERATIVE_MIN_MICROSECONDS;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class TaskletExecutionService {

    public static final String TASKLET_INIT_CLOSE_EXECUTOR_NAME = "jet:tasklet_initClose";

    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final ExecutionService hzExecutionService;
    private final CooperativeWorker[] cooperativeWorkers;
    private final Thread[] cooperativeThreadPool;
    private final String hzInstanceName;
    private final ILogger logger;
    private int cooperativeThreadIndex;
    @Probe(name = "blockingWorkerCount")
    private final Counter blockingWorkerCount = MwCounter.newMwCounter();
    private volatile boolean isShutdown;
    private final Object lock = new Object();
    private final IdleStrategy idlerCooperative;
    private final IdleStrategy idlerNonCooperative;

    public TaskletExecutionService(NodeEngineImpl nodeEngine, int threadCount, HazelcastProperties properties) {
        hzExecutionService = nodeEngine.getExecutionService();
        hzExecutionService.register(TASKLET_INIT_CLOSE_EXECUTOR_NAME,
                RuntimeAvailableProcessors.get(), Integer.MAX_VALUE, CACHED);
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.cooperativeWorkers = new CooperativeWorker[threadCount];
        this.cooperativeThreadPool = new Thread[threadCount];
        this.logger = nodeEngine.getLoggingService().getLogger(TaskletExecutionService.class);

        idlerCooperative = createIdler(
            properties, JET_IDLE_COOPERATIVE_MIN_MICROSECONDS, JET_IDLE_COOPERATIVE_MAX_MICROSECONDS
        );
        idlerNonCooperative = createIdler(
            properties, JET_IDLE_NONCOOPERATIVE_MIN_MICROSECONDS, JET_IDLE_NONCOOPERATIVE_MAX_MICROSECONDS
        );

        Arrays.setAll(cooperativeWorkers, i -> new CooperativeWorker());
        Arrays.setAll(cooperativeThreadPool, i -> new Thread(cooperativeWorkers[i],
                String.format("hz.%s.jet.cooperative.thread-%d", hzInstanceName, i)));
        Arrays.stream(cooperativeThreadPool).forEach(Thread::start);

        // register metrics
        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor = registry.newMetricDescriptor()
                                                     .withTag(MetricTags.MODULE, "jet");
        registry.registerStaticMetrics(descriptor, this);
        for (int i = 0; i < cooperativeWorkers.length; i++) {
            registry.registerStaticMetrics(
                    descriptor.withDiscriminator(MetricTags.COOPERATIVE_WORKER, String.valueOf(i)),
                    cooperativeWorkers[i]
            );
        }
    }

    /**
     * Submits the tasklets for execution and returns a future which gets
     * completed when the execution of all the tasklets has completed. If an
     * exception occurs or the execution gets cancelled, the future will be
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
        final ExecutionTracker executionTracker = new ExecutionTracker(tasklets.size(), cancellationFuture);
        try {
            final Map<Boolean, List<Tasklet>> byCooperation =
                    tasklets.stream().collect(partitioningBy(
                            tasklet -> doWithClassLoader(jobClassLoader, tasklet::isCooperative)
                    ));
            submitCooperativeTasklets(executionTracker, jobClassLoader, byCooperation.get(true));
            submitBlockingTasklets(executionTracker, jobClassLoader, byCooperation.get(false));
        } catch (Throwable t) {
            executionTracker.future.internalCompleteExceptionally(t);
        }
        return executionTracker.future;
    }

    public void shutdown() {
        isShutdown = true;
        Arrays.stream(cooperativeWorkers).forEach(thread -> thread.newTaskletSemaphore.release());
        blockingTaskletExecutor.shutdownNow();
        hzExecutionService.shutdownExecutor(TASKLET_INIT_CLOSE_EXECUTOR_NAME);
    }

    private void submitBlockingTasklets(ExecutionTracker executionTracker, ClassLoader jobClassLoader,
                                        List<Tasklet> tasklets) {
        CountDownLatch startedLatch = new CountDownLatch(tasklets.size());
        executionTracker.blockingFutures = tasklets
                .stream()
                .map(t -> new BlockingWorker(new TaskletTracker(t, executionTracker, jobClassLoader), startedLatch))
                .map(blockingTaskletExecutor::submit)
                .collect(toList());

        // Do not return from this method until all workers have started. Otherwise
        // on cancellation there is a race where the executor might not have started
        // the worker yet. This would result in taskletDone() never being called for
        // a worker.
        uncheckRun(startedLatch::await);
    }

    private void submitCooperativeTasklets(
            ExecutionTracker executionTracker, ClassLoader jobClassLoader, List<Tasklet> tasklets
    ) {
        @SuppressWarnings("unchecked")
        final List<TaskletTracker>[] trackersByThread = new List[cooperativeWorkers.length];
        Arrays.setAll(trackersByThread, i -> new ArrayList<>());
        List<? extends Future<?>> futures = tasklets
                .stream()
                .map(tasklet -> hzExecutionService.submit(TASKLET_INIT_CLOSE_EXECUTOR_NAME, () ->
                        Util.doWithClassLoader(jobClassLoader, tasklet::init)))
                .collect(toList());
        awaitAll(futures);

        // We synchronize so that no two jobs submit their tasklets in
        // parallel. If two jobs submit in parallel, the tasklets of one of
        // them could happen to not use all threads. When the other one ends,
        // some worker might have no tasklet.
        synchronized (lock) {
            for (Tasklet t : tasklets) {
                trackersByThread[cooperativeThreadIndex].add(new TaskletTracker(t, executionTracker, jobClassLoader));
                cooperativeThreadIndex = (cooperativeThreadIndex + 1) % trackersByThread.length;
            }
        }
        for (int i = 0; i < trackersByThread.length; i++) {
            cooperativeWorkers[i].trackers.addAll(0, trackersByThread[i]);
            cooperativeWorkers[i].newTaskletSemaphore.release();
        }
        Arrays.stream(cooperativeThreadPool).forEach(LockSupport::unpark);
    }

    private void awaitAll(List<? extends Future<?>> futures) {
        Throwable firstFailure = null;
        int failureCount = 0;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                Throwable peeled = peel(e);
                logger.severe("Tasklet initialization failed", peeled);
                firstFailure = firstFailure != null ? firstFailure : peeled;
                failureCount++;
            }
        }
        if (firstFailure != null) {
            throw new JetException(String.format(
                    "%,d of %,d tasklets failed to initialize." +
                            " One of the failures is attached as the cause and its summary is %s",
                    failureCount, futures.size(), firstFailure
            ), firstFailure);
        }
    }

    /**
     * Blocks until all workers terminate (cooperative & blocking).
     */
    public void awaitWorkerTermination() {
        assert isShutdown : "Not shut down";
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

    private BackoffIdleStrategy createIdler(
        HazelcastProperties props, HazelcastProperty minProp, HazelcastProperty maxProp
    ) {
        int min = props.getInteger(minProp);
        int max = props.getInteger(maxProp);
        String minName = minProp.getName();
        String maxName = maxProp.getName();
        if (min >= max) {
            logger.warning(
                String.format(
                    "The property %s must be set less than or equal to %s but current values are: %s=%d, %s=%d." +
                        " Using minimum value as maximum instead.",
                    minName, maxName, minName, min, maxName, max));
            max = min;
        }

        logFinest(logger, "Creating idler with %s=%dµs,%s=%dµs", minName, min, maxName, max);
        return new BackoffIdleStrategy(0, 0,
            minProp.getTimeUnit().toNanos(min), maxProp.getTimeUnit().toNanos(max)
        );
    }

    private void handleTaskletExecutionError(TaskletTracker t, Throwable e) {
        if (e instanceof CancellationException) {
            logger.fine("Job was cancelled by the user.");
            t.executionTracker.exception(e);
        } else if (e instanceof ResultLimitReachedException) {
            logger.fine("SQL LIMIT reached.");
            t.executionTracker.exception(e);
        } else {
            logger.info("Exception in " + t.tasklet, e);
            t.executionTracker.exception(new JetException("Exception in " + t.tasklet + ": " + e, e));
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
            currentThread().setContextClassLoader(tracker.jobClassLoader);
            IdleStrategy idlerLocal = idlerNonCooperative;
            Contexts.Container contextContainer = Contexts.container();

            try {
                blockingWorkerCount.inc();
                contextContainer.setContext(t.getProcessorContext());
                startedLatch.countDown();
                t.init();
                long idleCount = 0;
                ProgressState result;
                do {
                    result = t.call();
                    if (result.isMadeProgress()) {
                        idleCount = 0;
                    } else {
                        idlerLocal.idle(++idleCount);
                    }
                } while (!result.isDone()
                        && !tracker.executionTracker.executionCompletedExceptionally()
                        && !isShutdown);
            } catch (Throwable e) {
                handleTaskletExecutionError(tracker, e);
            } finally {
                blockingWorkerCount.inc(-1L);
                contextContainer.setContext(null);
                currentThread().setContextClassLoader(clBackup);
                tracker.executionTracker.taskletDone();
            }
        }
    }

    private final class CooperativeWorker implements Runnable {
        private static final int COOPERATIVE_LOGGING_THRESHOLD = 5;

        @Probe(name = "taskletCount")
        private final CopyOnWriteArrayList<TaskletTracker> trackers;
        @Probe(name = "iterationCount")
        private final Counter iterationCount = SwCounter.newSwCounter();

        private final ProgressTracker progressTracker = new ProgressTracker();
        // prevent lambda allocation on each iteration
        private final Consumer<TaskletTracker> runTasklet = this::runTasklet;

        private final Semaphore newTaskletSemaphore = new Semaphore(0);

        private boolean finestLogEnabled;
        private Thread myThread;
        private Contexts.Container contextContainer;

        CooperativeWorker() {
            this.trackers = new CopyOnWriteArrayList<>();
        }

        @Override
        public void run() {
            myThread = currentThread();
            contextContainer = Contexts.container();

            IdleStrategy idlerLocal = idlerCooperative;
            long idleCount = 0;

            while (!isShutdown) {
                finestLogEnabled = logger.isFinestEnabled();
                progressTracker.reset();
                // garbage-free iteration -- relies on implementation in COWArrayList that doesn't use an Iterator
                trackers.forEach(runTasklet);
                iterationCount.inc();
                if (!progressTracker.isMadeProgress() && newTaskletSemaphore.drainPermits() > 0) {
                    progressTracker.madeProgress();
                }
                if (progressTracker.isMadeProgress()) {
                    idleCount = 0;
                } else {
                    if (trackers.isEmpty()) {
                        newTaskletSemaphore.drainPermits();
                        if (trackers.isEmpty() && !isShutdown) {
                            try {
                                newTaskletSemaphore.acquire();
                            } catch (InterruptedException e) {
                                logger.severe("Cooperative worker interrupted", e);
                                return;
                            }
                        }
                    } else {
                        idlerLocal.idle(++idleCount);
                    }
                }
            }
            trackers.forEach(t -> t.executionTracker.taskletDone());
            trackers.clear();
        }

        private void runTasklet(TaskletTracker t) {
            long start = 0;
            if (finestLogEnabled) {
                start = System.nanoTime();
            }
            try {
                myThread.setContextClassLoader(t.jobClassLoader);
                contextContainer.setContext(t.tasklet.getProcessorContext());
                final ProgressState result = t.tasklet.call();
                if (result.isDone()) {
                    dismissTasklet(t);
                }
                progressTracker.mergeWith(result);
            } catch (Throwable e) {
                handleTaskletExecutionError(t, e);
            } finally {
                contextContainer.setContext(null);
            }
            if (t.executionTracker.executionCompletedExceptionally()) {
                dismissTasklet(t);
            }

            if (finestLogEnabled) {
                long elapsedMs = NANOSECONDS.toMillis((System.nanoTime() - start));
                if (elapsedMs > COOPERATIVE_LOGGING_THRESHOLD) {
                    logger.finest("Cooperative tasklet call of '" + t.tasklet + "' took more than "
                            + COOPERATIVE_LOGGING_THRESHOLD + " ms: " + elapsedMs + "ms");
                }
            }
        }

        private void dismissTasklet(TaskletTracker t) {
            logFinest(logger, "Tasklet %s is done", t.tasklet);
            t.executionTracker.taskletDone();
            trackers.remove(t);
        }
    }

    private static final class TaskletTracker {
        final Tasklet tasklet;
        final ExecutionTracker executionTracker;
        final ClassLoader jobClassLoader;

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
        volatile List<Future<?>> blockingFutures = emptyList();

        private final AtomicInteger completionLatch;
        private final AtomicReference<Throwable> executionException = new AtomicReference<>();

        ExecutionTracker(int taskletCount, CompletableFuture<Void> cancellationFuture) {
            this.completionLatch = new AtomicInteger(taskletCount);
            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                if (e == null) {
                    e = new IllegalStateException("cancellationFuture must be completed exceptionally");
                }
                exception(e);
                // Don't interrupt the threads. We require that they do not block for too long,
                // interrupting them might make the termination faster, but can also cause
                // troubles, for example as in https://github.com/hazelcast/hazelcast-jet/issues/1946
                blockingFutures.forEach(f -> f.cancel(false));
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

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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.BitSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_COMPLETED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_GENERIC_PRIORITY_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_GENERIC_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_GENERIC_THREAD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_PARTITION_THREAD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_PRIORITY_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_RUNNING_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_RUNNING_GENERIC_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_EXECUTOR_RUNNING_PARTITION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.spi.impl.operationservice.impl.InboundResponseHandlerSupplier.getIdleStrategy;
import static com.hazelcast.spi.properties.ClusterProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PRIORITY_GENERIC_OPERATION_THREAD_COUNT;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * An {@link OperationExecutor} that schedules:
 * <ol>
 * <li>partition-specific operations to a specific-partition operation thread (using a mod on the partition ID)</li>
 * <li>non-specific operations to generic operation threads</li>
 * </ol>
 * The {@link #execute(Object, int, boolean)} accepts an Object instead of a runnable to prevent needing to
 * create wrapper Runnables around tasks. This is done to reduce the amount of object litter and therefore
 * reduce the pressure on the GC.
 * <p>
 * There are 2 categories of operation threads:
 * <ol>
 * <li>partition-specific operation threads: these threads are responsible for executing e.g. a {@code map.put()}.
 * Operations for the same partition always end up in the same thread.
 * </li>
 * <li>
 * generic operation threads: these threads are responsible for executing operations that are not
 * specific to a partition, e.g. a heart beat.
 * </li>
 * </ol>
 */
@SuppressWarnings("checkstyle:methodcount")
public final class OperationExecutorImpl implements OperationExecutor, StaticMetricsProvider {
    private static final HazelcastProperty IDLE_STRATEGY
            = new HazelcastProperty("hazelcast.operation.partitionthread.idlestrategy", "block");
    private static final int TERMINATION_TIMEOUT_SECONDS = 3;
    private final ThreadAffinity threadAffinity = newSystemThreadAffinity("hazelcast.operation.thread.affinity");
    private final ILogger logger;

    // all operations for specific partitions will be executed on these threads, e.g. map.put(key, value)
    private final PartitionOperationThread[] partitionThreads;
    private final OperationRunner[] partitionOperationRunners;

    private final OperationQueue genericQueue
            = new OperationQueueImpl(new LinkedBlockingQueue<>(), new LinkedBlockingQueue<>());

    // all operations that are not specific for a partition will be executed here, e.g. heartbeat or map.size()
    private final GenericOperationThread[] genericThreads;
    private final OperationRunner[] genericOperationRunners;

    private final Address thisAddress;
    private final OperationRunner adHocOperationRunner;
    private final int priorityThreadCount;

    public OperationExecutorImpl(HazelcastProperties properties,
                                 LoggingService loggerService,
                                 Address thisAddress,
                                 OperationRunnerFactory runnerFactory,
                                 NodeExtension nodeExtension,
                                 String hzName,
                                 ClassLoader configClassLoader) {
        this.thisAddress = thisAddress;
        this.logger = loggerService.getLogger(OperationExecutorImpl.class);

        this.adHocOperationRunner = runnerFactory.createAdHocRunner();

        this.partitionOperationRunners = initPartitionOperationRunners(properties, runnerFactory);
        this.partitionThreads = initPartitionThreads(properties, hzName, nodeExtension, configClassLoader);

        this.priorityThreadCount = properties.getInteger(PRIORITY_GENERIC_OPERATION_THREAD_COUNT);
        this.genericOperationRunners = initGenericOperationRunners(properties, runnerFactory);
        this.genericThreads = initGenericThreads(hzName, nodeExtension, configClassLoader);
    }

    private OperationRunner[] initPartitionOperationRunners(HazelcastProperties properties,
                                                            OperationRunnerFactory runnerFactory) {
        OperationRunner[] operationRunners = new OperationRunner[properties.getInteger(PARTITION_COUNT)];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = runnerFactory.createPartitionRunner(partitionId);
        }
        return operationRunners;
    }

    private OperationRunner[] initGenericOperationRunners(HazelcastProperties properties, OperationRunnerFactory runnerFactory) {
        int threadCount = properties.getInteger(GENERIC_OPERATION_THREAD_COUNT);
        OperationRunner[] operationRunners = new OperationRunner[threadCount + priorityThreadCount];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = runnerFactory.createGenericRunner();
        }

        return operationRunners;
    }

    private PartitionOperationThread[] initPartitionThreads(HazelcastProperties properties, String hzName,
                                                            NodeExtension nodeExtension, ClassLoader configClassLoader) {

        int threadCount = properties.getInteger(PARTITION_OPERATION_THREAD_COUNT);
        if (threadAffinity.isEnabled()) {
            threadCount = threadAffinity.getThreadCount();
        }

        IdleStrategy idleStrategy = getIdleStrategy(properties, IDLE_STRATEGY);
        PartitionOperationThread[] threads = new PartitionOperationThread[threadCount];
        for (int threadId = 0; threadId < threads.length; threadId++) {
            String threadName = createThreadPoolName(hzName, "partition-operation") + threadId;
            // the normalQueue will be a blocking queue. We don't want to idle, because there are many operation threads.
            MPSCQueue<Object> normalQueue = new MPSCQueue<>(idleStrategy);

            OperationQueue operationQueue = new OperationQueueImpl(normalQueue, new ConcurrentLinkedQueue<>());

            PartitionOperationThread partitionThread = new PartitionOperationThread(threadName, threadId, operationQueue, logger,
                    nodeExtension, partitionOperationRunners, configClassLoader);
            partitionThread.setThreadAffinity(threadAffinity);
            threads[threadId] = partitionThread;
            normalQueue.setConsumerThread(partitionThread);
        }

        // we need to assign the PartitionOperationThreads to all OperationRunners they own
        for (int partitionId = 0; partitionId < partitionOperationRunners.length; partitionId++) {
            int threadId = getPartitionThreadId(partitionId, threadCount);
            Thread thread = threads[threadId];
            OperationRunner runner = partitionOperationRunners[partitionId];
            runner.setCurrentThread(thread);
        }

        return threads;
    }

    static int getPartitionThreadId(int partitionId, int partitionThreadCount) {
        return partitionId % partitionThreadCount;
    }

    private GenericOperationThread[] initGenericThreads(String hzName, NodeExtension nodeExtension,
                                                        ClassLoader configClassLoader) {
        // we created as many generic operation handlers, as there are generic threads
        int threadCount = genericOperationRunners.length;

        GenericOperationThread[] threads = new GenericOperationThread[threadCount];

        int threadId = 0;
        for (int threadIndex = 0; threadIndex < threads.length; threadIndex++) {
            boolean priority = threadIndex < priorityThreadCount;
            String baseName = priority ? "priority-generic-operation" : "generic-operation";
            String threadName = createThreadPoolName(hzName, baseName) + threadId;
            OperationRunner operationRunner = genericOperationRunners[threadIndex];

            GenericOperationThread operationThread = new GenericOperationThread(
                    threadName, threadIndex, genericQueue, logger, nodeExtension, operationRunner, priority, configClassLoader);

            threads[threadIndex] = operationThread;
            operationRunner.setCurrentThread(operationThread);

            if (threadIndex == priorityThreadCount - 1) {
                threadId = 0;
            } else {
                threadId++;
            }
        }

        return threads;
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, OPERATION_PREFIX);

        registry.provideMetrics((Object[]) genericThreads);
        registry.provideMetrics((Object[]) partitionThreads);
        registry.provideMetrics(adHocOperationRunner);
        registry.provideMetrics((Object[]) genericOperationRunners);
        registry.provideMetrics((Object[]) partitionOperationRunners);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    public OperationRunner[] getPartitionOperationRunners() {
        return partitionOperationRunners;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    public OperationRunner[] getGenericOperationRunners() {
        return genericOperationRunners;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        scan(partitionOperationRunners, liveOperations);
        scan(genericOperationRunners, liveOperations);
    }

    private void scan(OperationRunner[] runners, LiveOperations result) {
        for (OperationRunner runner : runners) {
            Object task = runner.currentTask();
            if (!(task instanceof Operation) || task.getClass() == Backup.class) {
                continue;
            }
            Operation operation = (Operation) task;
            result.add(operation.getCallerAddress(), operation.getCallId());
        }
    }

    @Probe(name = OPERATION_METRIC_EXECUTOR_RUNNING_COUNT)
    @Override
    public int getRunningOperationCount() {
        return getRunningPartitionOperationCount() + getRunningGenericOperationCount();
    }

    @Probe(name = OPERATION_METRIC_EXECUTOR_RUNNING_PARTITION_COUNT)
    private int getRunningPartitionOperationCount() {
        return getRunningOperationCount(partitionOperationRunners);
    }

    @Probe(name = OPERATION_METRIC_EXECUTOR_RUNNING_GENERIC_COUNT)
    private int getRunningGenericOperationCount() {
        return getRunningOperationCount(genericOperationRunners);
    }

    private static int getRunningOperationCount(OperationRunner[] runners) {
        int result = 0;
        for (OperationRunner runner : runners) {
            if (runner.currentTask() != null) {
                result++;
            }
        }
        return result;
    }

    @Override
    @Probe(name = OPERATION_METRIC_EXECUTOR_QUEUE_SIZE, level = MANDATORY)
    public int getQueueSize() {
        int size = 0;
        for (PartitionOperationThread partitionThread : partitionThreads) {
            size += partitionThread.queue.normalSize();
        }
        size += genericQueue.normalSize();
        return size;
    }

    @Override
    @Probe(name = OPERATION_METRIC_EXECUTOR_PRIORITY_QUEUE_SIZE, level = MANDATORY)
    public int getPriorityQueueSize() {
        int size = 0;
        for (PartitionOperationThread partitionThread : partitionThreads) {
            size += partitionThread.queue.prioritySize();
        }
        size += genericQueue.prioritySize();
        return size;
    }

    @Probe(name = OPERATION_METRIC_EXECUTOR_GENERIC_QUEUE_SIZE)
    private int getGenericQueueSize() {
        return genericQueue.normalSize();
    }

    @Probe(name = OPERATION_METRIC_EXECUTOR_GENERIC_PRIORITY_QUEUE_SIZE)
    private int getGenericPriorityQueueSize() {
        return genericQueue.prioritySize();
    }

    @Probe(name = OPERATION_METRIC_EXECUTOR_COMPLETED_COUNT, level = MANDATORY)
    public long getExecutedOperationCount() {
        long result = adHocOperationRunner.executedOperationsCount();

        for (OperationRunner runner : genericOperationRunners) {
            result += runner.executedOperationsCount();
        }

        for (OperationRunner runner : partitionOperationRunners) {
            result += runner.executedOperationsCount();
        }

        return result;
    }

    @Override
    @Probe(name = OPERATION_METRIC_EXECUTOR_PARTITION_THREAD_COUNT)
    public int getPartitionThreadCount() {
        return partitionThreads.length;
    }

    @Override
    @Probe(name = OPERATION_METRIC_EXECUTOR_GENERIC_THREAD_COUNT)
    public int getGenericThreadCount() {
        return genericThreads.length;
    }

    @Override
    public int getPartitionThreadId(int partitionId) {
        return getPartitionThreadId(partitionId, partitionThreads.length);
    }

    @Override
    public void execute(Operation op) {
        checkNotNull(op, "op can't be null");

        execute(op, op.getPartitionId(), op.isUrgent());
    }

    @Override
    public void executeOnPartitions(PartitionTaskFactory taskFactory, BitSet partitions) {
        checkNotNull(taskFactory, "taskFactory can't be null");
        checkNotNull(partitions, "partitions can't be null");

        for (PartitionOperationThread partitionThread : partitionThreads) {
            TaskBatch batch = new TaskBatch(taskFactory, partitions, partitionThread.threadId, partitionThreads.length);
            partitionThread.queue.add(batch, false);
        }
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        checkNotNull(task, "task can't be null");

        execute(task, task.getPartitionId(), task instanceof UrgentSystemOperation);
    }

    @Override
    public void accept(Packet packet) {
        execute(packet, packet.getPartitionId(), packet.isUrgent());
    }

    private void execute(Object task, int partitionId, boolean priority) {
        if (partitionId < 0) {
            genericQueue.add(task, priority);
        } else {
            OperationThread partitionThread = partitionThreads[toPartitionThreadIndex(partitionId)];
            partitionThread.queue.add(task, priority);
        }
    }

    @Override
    public void executeOnPartitionThreads(Runnable task) {
        checkNotNull(task, "task can't be null");
        boolean priority = task instanceof UrgentSystemOperation;

        for (OperationThread partitionThread : partitionThreads) {
            partitionThread.queue.add(task, priority);
        }
    }

    @Override
    public void run(Operation operation) {
        checkNotNull(operation, "operation can't be null");

        if (!isRunAllowed(operation)) {
            throw new IllegalThreadStateException("Operation '" + operation + "' cannot be run in current thread: "
                    + Thread.currentThread());
        }

        OperationRunner operationRunner = getOperationRunner(operation);
        operationRunner.run(operation);
    }

    OperationRunner getOperationRunner(Operation operation) {
        checkNotNull(operation, "operation can't be null");

        if (operation.getPartitionId() >= 0) {
            // retrieving an OperationRunner for a partition specific operation is easy; we can just use the partition ID.
            return partitionOperationRunners[operation.getPartitionId()];
        }

        Thread currentThread = Thread.currentThread();
        if (!(currentThread instanceof OperationThread)) {
            // if thread is not an operation thread, we return the adHocOperationRunner
            return adHocOperationRunner;
        }

        // It is a generic operation and we are running on an operation-thread. So we can just return the operation-runner
        // for that thread. There won't be any partition-conflict since generic operations are allowed to be executed by
        // a partition-specific operation-runner.
        OperationThread operationThread = (OperationThread) currentThread;
        return operationThread.currentRunner;
    }

    @Override
    public void runOrExecute(Operation op) {
        if (isRunAllowed(op)) {
            run(op);
        } else {
            execute(op);
        }
    }

    @Override
    public boolean isRunAllowed(Operation op) {
        checkNotNull(op, "op can't be null");

        Thread currentThread = Thread.currentThread();

        // IO threads are not allowed to run any operation
        if (currentThread instanceof OperationHostileThread) {
            return false;
        }

        int partitionId = op.getPartitionId();
        // TODO: do we want to allow non partition specific tasks to be run on a partitionSpecific operation thread?
        if (partitionId < 0) {
            return true;
        }

        // we are only allowed to execute partition aware actions on an OperationThread
        if (currentThread.getClass() != PartitionOperationThread.class) {
            return false;
        }

        PartitionOperationThread partitionThread = (PartitionOperationThread) currentThread;

        // so it's a partition operation thread, now we need to make sure that this operation thread is allowed
        // to execute operations for this particular partitionId
        return toPartitionThreadIndex(partitionId) == partitionThread.threadId;
    }

    @Override
    public boolean isInvocationAllowed(Operation op, boolean isAsync) {
        checkNotNull(op, "op can't be null");

        Thread currentThread = Thread.currentThread();

        // IO threads are not allowed to run any operation
        if (currentThread instanceof OperationHostileThread) {
            return false;
        }

        // if it is async we don't need to check if it is PartitionOperationThread or not
        if (isAsync) {
            return true;
        }

        // allowed to invoke non partition specific task
        if (op.getPartitionId() < 0) {
            return true;
        }

        // allowed to invoke from non PartitionOperationThreads (including GenericOperationThread)
        if (currentThread.getClass() != PartitionOperationThread.class) {
            return true;
        }

        PartitionOperationThread partitionThread = (PartitionOperationThread) currentThread;
        OperationRunner runner = partitionThread.currentRunner;
        if (runner != null) {
            // non null runner means it's a nested call
            // in this case partitionId of both inner and outer operations have to match
            return runner.getPartitionId() == op.getPartitionId();
        }

        return toPartitionThreadIndex(op.getPartitionId()) == partitionThread.threadId;
    }

    // public for testing purposes
    public int toPartitionThreadIndex(int partitionId) {
        return partitionId % partitionThreads.length;
    }

    @Override
    public void start() {
        if (logger.isFineEnabled()) {
            logger.fine("Starting " + partitionThreads.length + " partition threads and "
                  + genericThreads.length + " generic threads (" + priorityThreadCount + " dedicated for priority tasks)");
        }
        startAll(partitionThreads);
        startAll(genericThreads);
    }

    private static void startAll(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            thread.start();
        }
    }

    @Override
    public void shutdown() {
        shutdownAll(partitionThreads);
        shutdownAll(genericThreads);
        awaitTermination(partitionThreads);
        awaitTermination(genericThreads);
    }

    private static void shutdownAll(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            thread.shutdown();
        }
    }

    private static void awaitTermination(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            try {
                thread.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return "OperationExecutorImpl{node=" + thisAddress + '}';
    }
}

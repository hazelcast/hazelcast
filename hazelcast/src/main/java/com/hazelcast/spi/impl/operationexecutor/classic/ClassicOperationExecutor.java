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

package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.util.concurrent.NoOpIdleStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A {@link com.hazelcast.spi.impl.operationexecutor.OperationExecutor} that schedules:
 * <ol>
 * <li>partition specific operations to a specific partition-operation-thread (using a mod on the partition-id)</li>
 * <li>non specific operations to generic-operation-threads</li>
 * </ol>
 * The {@link #execute(Object, int, boolean)} accepts an Object instead of a runnable to prevent needing to
 * create wrapper runnables around tasks. This is done to reduce the amount of object litter and therefor
 * reduce pressure on the gc.
 * <p/>
 * There are 2 category of operation threads:
 * <ol>
 * <li>partition specific operation threads: these threads are responsible for executing e.g. a map.put.
 * Operations for the same partition, always end up in the same thread.
 * </li>
 * <li>
 * generic operation threads: these threads are responsible for executing operations that are not
 * specific to a partition. E.g. a heart beat.
 * </li>
 * </ol>
 */
public final class ClassicOperationExecutor implements OperationExecutor {

    public static final int TERMINATION_TIMEOUT_SECONDS = 3;

    private final ILogger logger;

    // all operations for specific partitions will be executed on these threads, e.g. map.put(key, value)
    private final PartitionOperationThread[] partitionOperationThreads;
    private final OperationRunner[] partitionOperationRunners;

    private final ScheduleQueue genericScheduleQueue;

    // all operations that are not specific for a partition will be executed here, e.g. heartbeat or map.size()
    private final GenericOperationThread[] genericOperationThreads;
    private final OperationRunner[] genericOperationRunners;

    private final Address thisAddress;
    private final NodeExtension nodeExtension;
    private final HazelcastThreadGroup threadGroup;
    private final OperationRunner adHocOperationRunner;
    private final MetricsRegistry metricsRegistry;

    public ClassicOperationExecutor(GroupProperties properties,
                                    LoggingService loggerService,
                                    Address thisAddress,
                                    OperationRunnerFactory operationRunnerFactory,
                                    HazelcastThreadGroup hazelcastThreadGroup,
                                    NodeExtension nodeExtension,
                                    MetricsRegistry metricsRegistry) {
        this.thisAddress = thisAddress;
        this.nodeExtension = nodeExtension;
        this.threadGroup = hazelcastThreadGroup;
        this.metricsRegistry = metricsRegistry;
        this.logger = loggerService.getLogger(ClassicOperationExecutor.class);
        this.genericScheduleQueue = new DefaultScheduleQueue();

        this.adHocOperationRunner = operationRunnerFactory.createAdHocRunner();

        this.partitionOperationRunners = initPartitionOperationRunners(properties, operationRunnerFactory);
        this.partitionOperationThreads = initPartitionThreads(properties);

        this.genericOperationRunners = initGenericOperationRunners(properties, operationRunnerFactory);
        this.genericOperationThreads = initGenericThreads();

        logger.info("Starting with " + genericOperationThreads.length + " generic operation threads and "
                + partitionOperationThreads.length + " partition operation threads.");
    }

    public void start() {
        for (Thread t : partitionOperationThreads) {
            t.start();
        }
        for (Thread t : genericOperationThreads) {
            t.start();
        }
    }

    private OperationRunner[] initPartitionOperationRunners(GroupProperties properties, OperationRunnerFactory handlerFactory) {
        OperationRunner[] operationRunners = new OperationRunner[properties.getInteger(GroupProperty.PARTITION_COUNT)];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = handlerFactory.createPartitionRunner(partitionId);
        }
        return operationRunners;
    }

    private OperationRunner[] initGenericOperationRunners(GroupProperties properties, OperationRunnerFactory runnerFactory) {
        int genericThreadCount = properties.getInteger(GroupProperty.GENERIC_OPERATION_THREAD_COUNT);
        if (genericThreadCount <= 0) {
            // default generic operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            genericThreadCount = Math.max(2, coreSize / 2);
        }

        OperationRunner[] operationRunners = new OperationRunner[genericThreadCount];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = runnerFactory.createGenericRunner();
        }
        return operationRunners;
    }

    private PartitionOperationThread[] initPartitionThreads(GroupProperties properties) {
        int threadCount = properties.getInteger(GroupProperty.PARTITION_OPERATION_THREAD_COUNT);
        if (threadCount <= 0) {
            // default partition operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = Math.max(2, coreSize);
        }

        PartitionOperationThread[] threads = new PartitionOperationThread[threadCount];
        for (int threadId = 0; threadId < threads.length; threadId++) {
            String threadName = threadGroup.getThreadPoolNamePrefix("partition-operation") + threadId;

            MPSCQueue normalQueue = new MPSCQueue(null, new NoOpIdleStrategy());
            ScheduleQueue scheduleQueue = new DefaultScheduleQueue(
                    normalQueue, new ConcurrentLinkedQueue());

            PartitionOperationThread operationThread = new PartitionOperationThread(threadName, threadId, scheduleQueue, logger,
                    threadGroup, nodeExtension, partitionOperationRunners);

            normalQueue.setOwningThread(operationThread);
            threads[threadId] = operationThread;

            metricsRegistry.scanAndRegister(operationThread, "operation." + operationThread.getName());
        }

        // we need to assign the PartitionOperationThreads to all OperationRunners they own
        for (int partitionId = 0; partitionId < partitionOperationRunners.length; partitionId++) {
            int threadId = partitionId % threadCount;
            Thread thread = threads[threadId];
            OperationRunner runner = partitionOperationRunners[partitionId];
            runner.setCurrentThread(thread);
        }

        return threads;
    }

    private GenericOperationThread[] initGenericThreads() {
        // we created as many generic operation handlers, as there are generic threads
        int threadCount = genericOperationRunners.length;
        GenericOperationThread[] threads = new GenericOperationThread[threadCount];

        for (int threadId = 0; threadId < threads.length; threadId++) {
            String threadName = threadGroup.getThreadPoolNamePrefix("generic-operation") + threadId;
            OperationRunner operationRunner = genericOperationRunners[threadId];

            GenericOperationThread operationThread = new GenericOperationThread(
                    threadName, threadId, genericScheduleQueue,
                    logger, threadGroup, nodeExtension, operationRunner);

            threads[threadId] = operationThread;
            operationRunner.setCurrentThread(operationThread);
            metricsRegistry.scanAndRegister(operationThread, "operation." + operationThread.getName());
        }

        return threads;
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
    public boolean isAllowedToRunInCurrentThread(Operation op) {
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
        if (!(currentThread instanceof PartitionOperationThread)) {
            return false;
        }

        PartitionOperationThread partitionThread = (PartitionOperationThread) currentThread;

        // so it's a partition operation thread, now we need to make sure that this operation thread is allowed
        // to execute operations for this particular partitionId
        return toPartitionThreadIndex(partitionId) == partitionThread.threadId;
    }

    @Override
    public boolean isOperationThread() {
        return Thread.currentThread() instanceof OperationThread;
    }

    @Override
    public boolean isInvocationAllowedFromCurrentThread(Operation op, boolean isAsync) {
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
        if (!(currentThread instanceof PartitionOperationThread)) {
            return true;
        }

        PartitionOperationThread partitionThread = (PartitionOperationThread) currentThread;
        OperationRunner runner = partitionThread.getCurrentOperationRunner();
        if (runner != null) {
            // non null runner means it's a nested call
            // in this case partitionId of both inner and outer operations have to match
            return runner.getPartitionId() == op.getPartitionId();
        }

        return toPartitionThreadIndex(op.getPartitionId()) == partitionThread.threadId;
    }

    @Override
    public int getRunningOperationCount() {
        int result = 0;
        for (OperationRunner handler : partitionOperationRunners) {
            if (handler.currentTask() != null) {
                result++;
            }
        }
        for (OperationRunner handler : genericOperationRunners) {
            if (handler.currentTask() != null) {
                result++;
            }
        }
        return result;
    }

    @Override
    public int getOperationExecutorQueueSize() {
        int size = 0;

        for (PartitionOperationThread t : partitionOperationThreads) {
            size += t.scheduleQueue.normalSize();
        }

        size += genericScheduleQueue.normalSize();

        return size;
    }

    @Override
    public int getPriorityOperationExecutorQueueSize() {
        int size = 0;

        for (PartitionOperationThread t : partitionOperationThreads) {
            size += t.scheduleQueue.prioritySize();
        }

        size += genericScheduleQueue.prioritySize();
        return size;
    }

    @Override
    public int getPartitionOperationThreadCount() {
        return partitionOperationThreads.length;
    }

    @Override
    public int getGenericOperationThreadCount() {
        return genericOperationThreads.length;
    }

    @Override
    public void execute(Operation op) {
        checkNotNull(op, "op can't be null");

        execute(op, op.getPartitionId(), op.isUrgent());
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        checkNotNull(task, "task can't be null");

        execute(task, task.getPartitionId(), false);
    }

    @Override
    public void runOnCallingThreadIfPossible(Operation op) {
        if (isAllowedToRunInCurrentThread(op)) {
            runOnCallingThread(op);
        } else {
            execute(op);
        }
    }

    @Override public void runOnAllPartitionThreads(Runnable task) {
        checkNotNull(task, "task can't be null");

        for (OperationThread partitionOperationThread : partitionOperationThreads) {
            partitionOperationThread.scheduleQueue.addUrgent(task);
        }
    }

    @Override
    public void interruptAllPartitionThreads() {
        for (PartitionOperationThread thread : partitionOperationThreads) {
            thread.interrupt();
        }
    }

    @Override
    public void execute(Packet packet) {
        checkNotNull(packet, "packet can't be null");
        checkOpPacket(packet);

        int partitionId = packet.getPartitionId();
        boolean hasPriority = packet.isUrgent();
        execute(packet, partitionId, hasPriority);
    }

    private void checkOpPacket(Packet packet) {
        if (!packet.isFlagSet(Packet.FLAG_OP)) {
            throw new IllegalStateException("Packet " + packet + " doesn't have Packet.FLAG_OP set");
        }
    }

    @Override
    public void runOnCallingThread(Operation operation) {
        checkNotNull(operation, "operation can't be null");

        if (!isAllowedToRunInCurrentThread(operation)) {
            throw new IllegalThreadStateException("Operation '" + operation + "' cannot be run in current thread: "
                    + Thread.currentThread());
        }

        OperationRunner operationRunner = getOperationRunner(operation);
        operationRunner.run(operation);
    }

    OperationRunner getOperationRunner(Operation operation) {
        checkNotNull(operation, "operation can't be null");

        if (operation.getPartitionId() >= 0) {
            // retrieving an OperationRunner for a partition specific operation is easy; we can just use the partition id.
            return partitionOperationRunners[operation.getPartitionId()];
        }

        Thread thread = Thread.currentThread();
        if (!(thread instanceof OperationThread)) {
            // if thread is not an operation thread, we return the adHocOperationRunner
            return adHocOperationRunner;
        }

        // It is a generic operation and we are running on an operation-thread. So we can just return the operation-runner
        // for that thread. There won't be any partition-conflict since generic operations are allowed to be executed by
        // a partition-specific operation-runner.
        OperationThread operationThread = (OperationThread) thread;
        return operationThread.getCurrentOperationRunner();
    }

    private void execute(Object task, int partitionId, boolean priority) {
        ScheduleQueue scheduleQueue;

        if (partitionId < 0) {
            scheduleQueue = genericScheduleQueue;
        } else {
            OperationThread partitionOperationThread = partitionOperationThreads[toPartitionThreadIndex(partitionId)];
            scheduleQueue = partitionOperationThread.scheduleQueue;
        }

        if (priority) {
            scheduleQueue.addUrgent(task);
        } else {
            scheduleQueue.add(task);
        }
    }

    public int toPartitionThreadIndex(int partitionId) {
        return partitionId % partitionOperationThreads.length;
    }

    @Override
    public void shutdown() {
        shutdownAll(partitionOperationThreads);
        shutdownAll(genericOperationThreads);
        awaitTermination(partitionOperationThreads);
        awaitTermination(genericOperationThreads);
    }

    private static void shutdownAll(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            thread.shutdown();
        }
    }

    private static void awaitTermination(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            try {
                thread.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return "ClassicOperationExecutor{"
                + "node=" + thisAddress
                + '}';
    }
}

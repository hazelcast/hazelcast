/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.NIOThread;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.concurrent.TimeUnit;

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

    //all operations for specific partitions will be executed on these threads, .e.g map.put(key,value).
    private final PartitionOperationThread[] partitionOperationThreads;
    private final OperationRunner[] partitionOperationRunners;

    private final ScheduleQueue genericScheduleQueue;

    //all operations that are not specific for a partition will be executed here, e.g heartbeat or map.size
    private final GenericOperationThread[] genericOperationThreads;
    private final OperationRunner[] genericOperationRunners;

    private final ResponseThread responseThread;
    private final ResponsePacketHandler responsePacketHandler;
    private final Address thisAddress;
    private final NodeExtension nodeExtension;
    private final HazelcastThreadGroup threadGroup;
    private final OperationRunner adHocOperationRunner;

    public ClassicOperationExecutor(GroupProperties properties,
                                    LoggingService loggerService,
                                    Address thisAddress,
                                    OperationRunnerFactory operationRunnerFactory,
                                    ResponsePacketHandler responsePacketHandler,
                                    HazelcastThreadGroup hazelcastThreadGroup,
                                    NodeExtension nodeExtension) {
        this.thisAddress = thisAddress;
        this.nodeExtension = nodeExtension;
        this.threadGroup = hazelcastThreadGroup;
        this.logger = loggerService.getLogger(ClassicOperationExecutor.class);
        this.responsePacketHandler = responsePacketHandler;
        this.genericScheduleQueue = new DefaultScheduleQueue();

        this.adHocOperationRunner = operationRunnerFactory.createAdHocRunner();

        this.partitionOperationRunners = initPartitionOperationRunners(properties, operationRunnerFactory);
        this.partitionOperationThreads = initPartitionThreads(properties);

        this.genericOperationRunners = initGenericOperationRunners(properties, operationRunnerFactory);
        this.genericOperationThreads = initGenericThreads();

        this.responseThread = initResponseThread();

        logger.info("Starting with " + genericOperationThreads.length + " generic operation threads and "
                + partitionOperationThreads.length + " partition operation threads.");
    }

    private OperationRunner[] initGenericOperationRunners(GroupProperties properties, OperationRunnerFactory runnerFactory) {
        int genericThreadCount = properties.GENERIC_OPERATION_THREAD_COUNT.getInteger();
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

    private OperationRunner[] initPartitionOperationRunners(GroupProperties properties,
                                                            OperationRunnerFactory handlerFactory) {
        OperationRunner[] operationRunners = new OperationRunner[properties.PARTITION_COUNT.getInteger()];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = handlerFactory.createPartitionRunner(partitionId);
        }
        return operationRunners;
    }

    private ResponseThread initResponseThread() {
        ResponseThread thread = new ResponseThread(threadGroup, logger, responsePacketHandler);
        thread.start();
        return thread;
    }

    private PartitionOperationThread[] initPartitionThreads(GroupProperties properties) {
        int threadCount = properties.PARTITION_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            // default partition operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = Math.max(2, coreSize);
        }

        PartitionOperationThread[] threads = new PartitionOperationThread[threadCount];
        for (int threadId = 0; threadId < threads.length; threadId++) {
            String threadName = threadGroup.getThreadPoolNamePrefix("partition-operation") + threadId;
            ScheduleQueue scheduleQueue = new DefaultScheduleQueue();

            PartitionOperationThread operationThread = new PartitionOperationThread(
                    threadName, threadId, scheduleQueue, logger,
                    threadGroup, nodeExtension, partitionOperationRunners);

            // we need to assign the thread to all OperationRunners this thread owns.
            for (int partitionId = 0; partitionId < partitionOperationRunners.length; partitionId++) {
                if (partitionId % threadCount == threadId) {
                    OperationRunner operationRunner = partitionOperationRunners[partitionId];
                    operationRunner.setCurrentThread(operationThread);
                }
            }

            threads[threadId] = operationThread;
            operationThread.start();
        }

        return threads;
    }

    private GenericOperationThread[] initGenericThreads() {
        // we created as many generic operation handlers, as there are generic threads.
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
            operationThread.start();
        }

        return threads;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP" })
    @Override
    public OperationRunner[] getPartitionOperationRunners() {
        return partitionOperationRunners;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP" })
    @Override
    public OperationRunner[] getGenericOperationRunners() {
        return genericOperationRunners;
    }

    @Override
    public boolean isAllowedToRunInCurrentThread(Operation op) {
        if (op == null) {
            throw new NullPointerException("op can't be null");
        }

        Thread currentThread = Thread.currentThread();

        // IO threads are not allowed to run any operation
        if (currentThread instanceof NIOThread) {
            return false;
        }

        int partitionId = op.getPartitionId();
        //todo: do we want to allow non partition specific tasks to be run on a partitionSpecific operation thread?
        if (partitionId < 0) {
            return true;
        }

        //we are only allowed to execute partition aware actions on an OperationThread.
        if (!(currentThread instanceof PartitionOperationThread)) {
            return false;
        }

        PartitionOperationThread partitionThread = (PartitionOperationThread) currentThread;

        //so it is an partition operation thread, now we need to make sure that this operation thread is allowed
        //to execute operations for this particular partitionId.
        return toPartitionThreadIndex(partitionId) == partitionThread.threadId;
    }

    @Override
    public boolean isOperationThread() {
        return Thread.currentThread() instanceof OperationThread;
    }

    @Override
    public boolean isInvocationAllowedFromCurrentThread(Operation op) {
        if (op == null) {
            throw new NullPointerException("op can't be null");
        }

        Thread currentThread = Thread.currentThread();

        // IO threads are not allowed to run any operation
        if (currentThread instanceof NIOThread) {
            return false;
        }

        if (op.getPartitionId() < 0) {
            return true;
        }

        // we are allowed to invoke from non PartitionOperationThreads (including GenericOperationThread).
        if (!(currentThread instanceof PartitionOperationThread)) {
            return true;
        }

        // we are only allowed to invoke from a PartitionOperationThread if the operation belongs to that
        // PartitionOperationThread.
        PartitionOperationThread partitionThread = (PartitionOperationThread) currentThread;
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
    public int getResponseQueueSize() {
        return responseThread.workQueue.size();
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
        if (op == null) {
            throw new NullPointerException("op can't be null");
        }
        execute(op, op.getPartitionId(), op.isUrgent());
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }
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

    @Override
    public void execute(Packet packet) {
        if (packet == null) {
            throw new NullPointerException("packet can't be null");
        }

        if (!packet.isHeaderSet(Packet.HEADER_OP)) {
            throw new IllegalStateException("Packet " + packet + " doesn't have Packet.HEADER_OP set");
        }

        if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
            //it is an response packet.
            responseThread.workQueue.add(packet);
        } else {
            //it is an must be an operation packet
            int partitionId = packet.getPartitionId();
            boolean hasPriority = packet.isUrgent();
            execute(packet, partitionId, hasPriority);
        }
    }

    @Override
    public void runOnCallingThread(Operation op) {
        if (op == null) {
            throw new NullPointerException("op can't be null");
        }

        if (!isAllowedToRunInCurrentThread(op)) {
            throw new IllegalThreadStateException("Operation: " + op + " cannot be run in current thread! -> "
                    + Thread.currentThread());
        }

        //TODO: We need to find the correct operation handler.
        OperationRunner operationRunner = getCurrentThreadOperationRunner();
        operationRunner.run(op);
    }

    public OperationRunner getCurrentThreadOperationRunner() {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof OperationThread)) {
            return adHocOperationRunner;
        }

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

    private int toPartitionThreadIndex(int partitionId) {
        return partitionId % partitionOperationThreads.length;
    }

    @Override
    public void shutdown() {
        responseThread.shutdown();
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
    public void dumpPerformanceMetrics(StringBuffer sb) {
        for (int k = 0; k < partitionOperationThreads.length; k++) {
            OperationThread operationThread = partitionOperationThreads[k];
            sb.append(operationThread.getName())
                    .append(" processedCount=").append(operationThread.processedCount)
                    .append(" pendingCount=").append(operationThread.scheduleQueue.size())
                    .append('\n');
        }
        sb.append("pending generic operations ").append(genericScheduleQueue.size()).append('\n');
        for (int k = 0; k < genericOperationThreads.length; k++) {
            OperationThread operationThread = genericOperationThreads[k];
            sb.append(operationThread.getName())
                    .append(" processedCount=").append(operationThread.processedCount).append('\n');
        }
        sb.append(responseThread.getName())
                .append(" processedCount=").append(responseThread.processedResponses)
                .append(" pendingCount=").append(responseThread.workQueue.size()).append('\n');
    }

    @Override
    public String toString() {
        return "ClassicOperationExecutor{"
                + "node=" + thisAddress
                + '}';
    }
}

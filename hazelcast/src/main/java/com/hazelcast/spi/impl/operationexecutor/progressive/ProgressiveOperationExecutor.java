package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.NIOThread;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationexecutor.classic.DefaultScheduleQueue;
import com.hazelcast.spi.impl.operationexecutor.classic.GenericOperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.OperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.ResponseThread;
import com.hazelcast.spi.impl.operationexecutor.classic.ScheduleQueue;

import java.util.concurrent.atomic.AtomicReference;

public final class ProgressiveOperationExecutor implements OperationExecutor {

    final PartitionQueue[] partitionQueues;
    final ProgressiveScheduleQueue[] scheduleQueues;
    final ILogger logger;

    private final ScheduleQueue genericScheduleQueue = new DefaultScheduleQueue();
    private final ResponsePacketHandler responsePacketHandler;
    private final ResponseThread responseThread;
    private final PartitionOperationThread[] partitionOperationThreads;
    private final GenericOperationThread[] genericOperationThreads;
    private final boolean callerRunsEnabled;
    private final GroupProperties properties;
    private final HazelcastThreadGroup hazelcastThreadGroup;
    private final NodeExtension nodeExtension;
    private final OperationRunner adhocRunner;
    private final OperationRunner[] partitionRunners;
    private final OperationRunner[] genericRunners;

    public ProgressiveOperationExecutor(GroupProperties properties,
                                        LoggingService loggingService,
                                        OperationRunnerFactory operationRunnerFactory,
                                        NodeExtension nodeExtension,
                                        ResponsePacketHandler responsePacketHandler,
                                        HazelcastThreadGroup hazelcastThreadGroup) {
        this.hazelcastThreadGroup = hazelcastThreadGroup;
        this.nodeExtension = nodeExtension;
        this.responsePacketHandler = responsePacketHandler;
        this.properties = properties;
        this.logger = loggingService.getLogger(ProgressiveOperationExecutor.class);
        this.responseThread = initResponseThread();

        this.callerRunsEnabled = initCallerRuns();

        this.adhocRunner = operationRunnerFactory.createAdHocRunner();
        this.partitionRunners = initPartitionOperationHandlers(operationRunnerFactory);
        this.genericRunners = initGenericOperationHandlers(properties, operationRunnerFactory);

        this.scheduleQueues = initScheduleQueues();
        this.partitionQueues = initPartitionQueues();

        this.partitionOperationThreads = initPartitionThreads();
        this.genericOperationThreads = initGenericThreads();
    }

    private OperationRunner[] initGenericOperationHandlers(GroupProperties properties, OperationRunnerFactory handlerFactory) {
        int genericThreadCount = properties.GENERIC_OPERATION_THREAD_COUNT.getInteger();
        if (genericThreadCount <= 0) {
            // default generic operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            genericThreadCount = Math.max(2, coreSize / 2);
        }

        OperationRunner[] operationRunners = new OperationRunner[genericThreadCount];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = handlerFactory.createGenericRunner();
        }
        return operationRunners;
    }

    private OperationRunner[] initPartitionOperationHandlers(OperationRunnerFactory handlerFactory) {
        OperationRunner[] operationRunners = new OperationRunner[properties.PARTITION_COUNT.getInteger()];
        for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
            operationRunners[partitionId] = handlerFactory.createPartitionRunner(partitionId);
        }
        return operationRunners;
    }

    private boolean initCallerRuns() {
        boolean callerRunsEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.callerruns.enabled", "true"));
        logger.info("Caller runs enabled: " + callerRunsEnabled);
        return true;
    }

    private ResponseThread initResponseThread() {
        return new ResponseThread(hazelcastThreadGroup, logger, responsePacketHandler);
    }

    private PartitionQueue[] initPartitionQueues() {
        int partitionCount = properties.PARTITION_COUNT.getInteger();
        PartitionQueue[] partitionQueues = new PartitionQueue[partitionCount];
        int scheduleQueueIndex = 0;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            ProgressiveScheduleQueue scheduleQueue = scheduleQueues[scheduleQueueIndex % scheduleQueues.length];

            OperationRunner runner = partitionRunners[partitionId];
            partitionQueues[partitionId] = new PartitionQueue(
                    partitionId,
                    scheduleQueue,
                    runner);
            scheduleQueueIndex++;
        }
        return partitionQueues;
    }

    private ProgressiveScheduleQueue[] initScheduleQueues() {
        int threadCount = properties.PARTITION_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            // default partition operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = Math.max(2, coreSize);
        }

        ProgressiveScheduleQueue[] scheduleQueues = new ProgressiveScheduleQueue[threadCount];
        for (int k = 0; k < scheduleQueues.length; k++) {
            scheduleQueues[k] = new ProgressiveScheduleQueue();
        }
        return scheduleQueues;
    }

    private GenericOperationThread[] initGenericThreads() {
        int threadCount = properties.GENERIC_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            // default generic operation thread count
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = Math.max(2, coreSize / 2);
        }

        GenericOperationThread[] threads = new GenericOperationThread[threadCount];
        for (int k = 0; k < threadCount; k++) {
            OperationRunner runner = genericRunners[k];
            String threadName = hazelcastThreadGroup.getThreadPoolNamePrefix("generic-operation") + k;
            threads[k] = new GenericOperationThread(
                    threadName, k, genericScheduleQueue,
                    logger, hazelcastThreadGroup, nodeExtension, runner);
        }

        return threads;
    }

    private PartitionOperationThread[] initPartitionThreads() {
        int threadCount = scheduleQueues.length;
        PartitionOperationThread[] threads = new PartitionOperationThread[threadCount];
        for (int k = 0; k < threadCount; k++) {
            String threadName = hazelcastThreadGroup.getThreadPoolNamePrefix("partition-operation") + k;
            ProgressiveScheduleQueue scheduleQueue = scheduleQueues[k];
            PartitionOperationThread thread = new PartitionOperationThread(
                    threadName, k, scheduleQueue,
                    logger, hazelcastThreadGroup, nodeExtension, partitionRunners);
            scheduleQueue.setOwnerThread(thread);
            threads[k] = thread;
        }
        return threads;
    }

    @Override
    public int getRunningOperationCount() {
        int result = 0;
        for (OperationRunner handler : partitionRunners) {
            if (handler.currentTask() != null) {
                result++;
            }
        }
        for (OperationRunner handler : genericRunners) {
            if (handler.currentTask() != null) {
                result++;
            }
        }
        return result;
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return 0;
    }

    @Override
    public int getPriorityOperationExecutorQueueSize() {
        return 0;
    }

    @Override
    public int getPartitionOperationThreadCount() {
        return partitionOperationThreads.length;
    }

    @Override
    public int getResponseQueueSize() {
        return responseThread.getWorkQueue().size();
    }

    @Override
    public int getGenericOperationThreadCount() {
        return genericOperationThreads.length;
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
        //no-op for the time being
    }

    @Override
    public OperationRunner[] getPartitionOperationRunners() {
        return partitionRunners;
    }

    @Override
    public OperationRunner[] getGenericOperationRunners() {
        return genericRunners;
    }

    @Override
    public void runOnCallingThread(Operation op) {
        if (op == null) {
            throw new NullPointerException("op can't be null");
        }

        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof NIOThread) {
            throw new IllegalThreadStateException();
        }

        int partitionId = op.getPartitionId();
        OperationRunner operationRunner = getOperationRunner(currentThread);
        if (partitionId >= 0) {
            // so we are running a specific partition operation, then partition id's must match
            if (operationRunner.getPartitionId() != partitionId) {
                throw new IllegalThreadStateException();
            }
        }

        operationRunner.run(op);
    }

    private OperationRunner getOperationRunner(Thread currentThread) {
        OperationRunner runner = getOperationRunnerForThread(currentThread);
        if (runner != null) {
            return runner;
        }

        return adhocRunner;
    }

    private OperationRunner getOperationRunnerForThread(Thread currentThread) {
        if (currentThread instanceof OperationThread) {
            OperationThread operationThread = (OperationThread) currentThread;
            return operationThread.getCurrentOperationRunner();
        }

        OperationRunner threadLocalOperationHandler = OperationRunnerThreadLocal.getThreadLocalOperationRunner();
        if (threadLocalOperationHandler != null) {
            return threadLocalOperationHandler;
        }

        return null;
    }

    @Override
    public boolean isOperationThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void runOnCallingThreadIfPossible(Operation op) {
        if (op == null) {
            throw new NullPointerException("op can't be null");
        }

        if (!callerRunsEnabled) {
            execute(op);
            return;
        }

        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof NIOThread) {
            // we always need to offload in case of an NIO thread.
            execute(op);
            return;
        }

        OperationRunner runner = getOperationRunnerForThread(currentThread);
        int partitionId = op.getPartitionId();
        if (runner == null) {
            // There is no runner. This means we are not running on an operation thread + we are not running
            // from a thread that is currently stealing.
            if (partitionId < 0) {
                // it is a generic operation
                // Since there is no handler, we execute on the ad hoc handler.
                adhocRunner.run(op);
            } else {
                // it is a partition specific operation. So we are going to try to schedule it with an assist.
                // It means that we are going to try to steal the partition and execute it ourselves.
                PartitionQueue partitionQueue = partitionQueues[partitionId];
                partitionQueue.runOrAdd(op);
            }
            return;
        }

        // There is nesting going on.

        if (partitionId < 0) {
            // We can safely execute generic operations on any operation handler.
            runner.run(op);
            return;
        }

        // so we are running a specific partition operation, then partition id's must match
        if (runner.getPartitionId() != partitionId) {
            // The partition id's don't match, so we need to offload the operation to a different thread.

            //TODO: It should be possible to let a partition specific operation be executed from a generic operation thread.
            // Then we'll be overriding the operation handler from the generic one by a partition specific

            execute(op);
            return;
        }

        // We have a nested operation. The outer operation is partition specific, and the inner operation as well
        // and both the partition-id's are the same. So we are going to execute it ourselves!s
        runner.run(op);
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
        OperationRunner operationRunner = getOperationRunner(currentThread);
        if (partitionId >= 0) {
            // so we are running a specific partition operation, then partition id's must match
            if (operationRunner.getPartitionId() != partitionId) {
                return false;
            }
        }

        return true;
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

        int partitionId = op.getPartitionId();
        if (partitionId < 0) {
            return true;
        }

        OperationRunner runner = getOperationRunner(currentThread);
        if (runner.getPartitionId() < 0) {
            return true;
        }

        return runner.getPartitionId() == partitionId;
    }

    @Override
    public void execute(Operation op) {
        int partitionId = op.getPartitionId();
        if (partitionId < 0) {
            if (op.isUrgent()) {
                genericScheduleQueue.addUrgent(op);
            } else {
                genericScheduleQueue.add(op);
            }
        } else {
            PartitionQueue partitionQueue = partitionQueues[partitionId];
            if (op.isUrgent()) {
                partitionQueue.priorityAdd(op);
            } else {
                partitionQueue.add(op);
            }
        }
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        int partitionId = task.getPartitionId();
        if (partitionId < 0) {
            genericScheduleQueue.add(task);
        } else {
            PartitionQueue partitionQueue = partitionQueues[partitionId];
            partitionQueue.add(task);
        }
    }

    @Override
    public void execute(Packet packet) {
        if (packet == null) {
            throw new NullPointerException("packet can't be null");
        }

        if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
            // it is an response packet.
            responseThread.add(packet);
            return;
        }

        // it is an must be an operation packet
        int partitionId = packet.getPartitionId();

        if (partitionId < 0) {
            if (packet.isUrgent()) {
                genericScheduleQueue.addUrgent(packet);
            } else {
                genericScheduleQueue.add(packet);
            }
            return;
        }

        PartitionQueue partitionQueue = partitionQueues[partitionId];
        if (packet.isUrgent()) {
            partitionQueue.priorityAdd(packet);
        } else {
            partitionQueue.add(packet);
        }
    }

    @Override
    public void start() {
        for (PartitionOperationThread t : partitionOperationThreads) {
            t.start();
        }

        for (GenericOperationThread t : genericOperationThreads) {
            t.start();
        }

        responseThread.start();
    }

    @Override
    public void shutdown() {
        for (GenericOperationThread t : genericOperationThreads) {
            t.shutdown();
        }

        for (PartitionOperationThread t : partitionOperationThreads) {
            t.shutdown();
        }
        responseThread.shutdown();
    }
}

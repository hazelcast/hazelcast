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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ExecutionTracingService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationexecutor.classic.ClassicOperationExecutor;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.spi.OperationAccessor.isJoinOperation;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setConnection;
import static com.hazelcast.spi.impl.ResponseHandlerFactory.setRemoteResponseHandler;
import static java.lang.Math.min;

/**
 * This is the Basic InternalOperationService and depends on Java 6.
 * <p/>
 * All the classes that begin with 'Basic' are implementation detail that depend on the
 * {@link com.hazelcast.spi.impl.BasicOperationService}.
 * <p/>
 * <h1>System Operation</h1>
 * When a {@link com.hazelcast.spi.UrgentSystemOperation} is invoked on this OperationService, it will be executed with a
 * high urgency by making use of a urgent queue. So when the system is under load, and the operation queues are
 * filled, then system operations are executed before normal operation. The advantage is that when a system is under
 * pressure, it still is able to do things like recognizing new members in the cluster and moving partitions around.
 * <p/>
 * When a UrgentSystemOperation is send to a remote machine, it is wrapped in a {@link Packet} and the packet is marked as a
 * urgent packet. When this packet is received on the remove OperationService, the urgent flag is checked and if
 * needed, the operation is set on the urgent queue. So local and remote execution of System operations will obey
 * the urgency.
 *
 * @see com.hazelcast.spi.impl.BasicInvocation
 * @see com.hazelcast.spi.impl.BasicInvocationBuilder
 * @see com.hazelcast.spi.impl.BasicPartitionInvocation
 * @see com.hazelcast.spi.impl.BasicTargetInvocation
 */
final class BasicOperationService implements InternalOperationService {

    private static final int INITIAL_CAPACITY = 1000;
    private static final float LOAD_FACTOR = 0.75f;
    private static final long SCHEDULE_DELAY = 1111;
    private static final int CORE_SIZE_CHECK = 8;
    private static final int CORE_SIZE_FACTOR = 4;
    private static final int CONCURRENCY_LEVEL = 16;
    private static final int ASYNC_QUEUE_CAPACITY = 100000;

    final ConcurrentMap<Long, BasicInvocation> invocations;
    final OperationExecutor operationExecutor;
    final ILogger invocationLogger;
    final ManagedExecutorService asyncExecutor;
    private final AtomicLong executedOperationsCount = new AtomicLong();

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong callIdGen = new AtomicLong(1);

    private final long defaultCallTimeoutMillis;
    private final long backupOperationTimeoutMillis;
    private final OperationBackupHandler operationBackupHandler;
    private final BasicBackPressureService backPressureService;
    private final BasicResponsePacketHandler responsePacketHandler;
    private volatile boolean shutdown;

    BasicOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class);
        this.invocationLogger = nodeEngine.getLogger(BasicInvocation.class);
        this.defaultCallTimeoutMillis = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        this.backupOperationTimeoutMillis = node.getGroupProperties().OPERATION_BACKUP_TIMEOUT_MILLIS.getLong();
        this.backPressureService = new BasicBackPressureService(node.getGroupProperties(), logger);

        int coreSize = Runtime.getRuntime().availableProcessors();
        boolean reallyMultiCore = coreSize >= CORE_SIZE_CHECK;
        int concurrencyLevel = reallyMultiCore ? coreSize * CORE_SIZE_FACTOR : CONCURRENCY_LEVEL;
        this.invocations = new ConcurrentHashMap<Long, BasicInvocation>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);
        this.responsePacketHandler = new BasicResponsePacketHandler();
        this.operationBackupHandler = new OperationBackupHandler();

        this.operationExecutor = new ClassicOperationExecutor(
                node.getGroupProperties(),
                node.loggingService,
                node.getThisAddress(),
                new BasicOperationRunnerFactory(),
                responsePacketHandler, node.getHazelcastThreadGroup(), node.getNodeExtension()
        );

        ExecutionService executionService = nodeEngine.getExecutionService();
        this.asyncExecutor = executionService.register(ExecutionService.ASYNC_EXECUTOR, coreSize,
                ASYNC_QUEUE_CAPACITY, ExecutorType.CONCRETE);

        startCleanupThread();
    }

    private void startCleanupThread() {
        CleanupThread t = new CleanupThread();
        t.start();
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
        operationExecutor.dumpPerformanceMetrics(sb);
    }

    @Override
    public int getPartitionOperationThreadCount() {
        return operationExecutor.getPartitionOperationThreadCount();
    }

    @Override
    public int getGenericOperationThreadCount() {
        return operationExecutor.getGenericOperationThreadCount();
    }

    @Override
    public int getRunningOperationsCount() {
        return operationExecutor.getRunningOperationCount();
    }

    @Override
    public long getExecutedOperationCount() {
        return executedOperationsCount.get();
    }

    @Override
    public int getRemoteOperationsCount() {
        return invocations.size();
    }

    @Override
    public int getResponseQueueSize() {
        return operationExecutor.getResponseQueueSize();
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return operationExecutor.getOperationExecutorQueueSize();
    }

    @Override
    public int getPriorityOperationExecutorQueueSize() {
        return operationExecutor.getPriorityOperationExecutorQueueSize();
    }

    @Override
    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        operationExecutor.execute(task);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id cannot be negative!");
        }
        return new BasicInvocationBuilder(nodeEngine, serviceName, op, partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null!");
        }
        return new BasicInvocationBuilder(nodeEngine, serviceName, op, target);
    }

    @Override
    public void runOperationOnCallingThread(Operation op) {
       operationExecutor.runOnCallingThread(op);
    }

    @Override
    public void executeOperation(Operation op) {
        operationExecutor.execute(op);
    }

    @Override
    public boolean isAllowedToRunOnCallingThread(Operation op) {
        return operationExecutor.isAllowedToRunInCurrentThread(op);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        return new BasicPartitionInvocation(nodeEngine, serviceName, op, partitionId, InvocationBuilder.DEFAULT_REPLICA_INDEX,
                InvocationBuilder.DEFAULT_TRY_COUNT, InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                InvocationBuilder.DEFAULT_CALL_TIMEOUT, null, InvocationBuilder.DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
         return new BasicTargetInvocation(nodeEngine, serviceName, op, target, InvocationBuilder.DEFAULT_TRY_COUNT,
                InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                InvocationBuilder.DEFAULT_CALL_TIMEOUT, null, InvocationBuilder.DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    // =============================== processing response  ===============================

    @Override
    public void notifyBackupCall(long callId) {
        try {
            final BasicInvocation invocation = invocations.get(callId);
            if (invocation != null) {
                invocation.signalOneBackupComplete();
            }
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    // =============================== processing operation  ===============================

    @Override
    public boolean isCallTimedOut(Operation op) {
        if (op.returnsResponse() && op.getCallId() != 0) {
            final long callTimeout = op.getCallTimeout();
            final long invocationTime = op.getInvocationTime();
            final long expireTime = invocationTime + callTimeout;
            if (expireTime > 0 && expireTime < Long.MAX_VALUE) {
                final long now = nodeEngine.getClusterService().getClusterClock().getClusterTime();
                if (expireTime < now) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        Map<Address, List<Integer>> memberPartitions = nodeEngine.getPartitionService().getMemberPartitionsMap();
        InvokeOnPartitions invokeOnPartitions = new InvokeOnPartitions(serviceName, operationFactory, memberPartitions);
        return invokeOnPartitions.invoke();
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                   Collection<Integer> partitions) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(3);
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int partition : partitions) {
            Address owner = partitionService.getPartitionOwnerOrWait(partition);
            if (!memberPartitions.containsKey(owner)) {
                memberPartitions.put(owner, new ArrayList<Integer>());
            }
            memberPartitions.get(owner).add(partition);
        }
        InvokeOnPartitions invokeOnPartitions = new InvokeOnPartitions(serviceName, operationFactory, memberPartitions);
        return invokeOnPartitions.invoke();
    }

    @Override
    public boolean send(Operation op, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }
        Data data = nodeEngine.toData(op);
        int partitionId = op.getPartitionId();
        Packet packet = new Packet(data, partitionId, nodeEngine.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        if (op instanceof UrgentSystemOperation) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        Connection connection = node.getConnectionManager().getOrConnect(target);
        return nodeEngine.getPacketTransceiver().transmit(packet, connection);
    }

    @Override
    public boolean send(Response response, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }
        Data data = nodeEngine.toData(response);
        Packet packet = new Packet(data, nodeEngine.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (response.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        Connection connection = node.getConnectionManager().getOrConnect(target);
        return nodeEngine.getPacketTransceiver().transmit(packet, connection);
    }

    public void registerInvocation(BasicInvocation invocation) {
        long callId = callIdGen.getAndIncrement();
        Operation op = invocation.op;
        if (op.getCallId() != 0) {
            invocations.remove(op.getCallId());
        }

        invocations.put(callId, invocation);
        setCallId(invocation.op, callId);
    }

    public void deregisterInvocation(BasicInvocation invocation) {
        long callId = invocation.op.getCallId();
        // locally executed non backup-aware operations (e.g. a map.get on a local member) doesn't have a call id.
        // so in that case we can skip the deregistration since it isn't registered in the first place.
        if (callId == 0) {
            return;
        }

        invocations.remove(callId);
    }

    @PrivateApi
    long getDefaultCallTimeoutMillis() {
        return defaultCallTimeoutMillis;
    }

    @Override
    public boolean isOperationExecuting(Address callerAddress, String callerUuid, String serviceName, Object identifier) {
        Object service = nodeEngine.getService(serviceName);
        if (service == null) {
            logger.severe("Not able to find operation execution info. Invalid service: " + serviceName);
            return false;
        }
        if (service instanceof ExecutionTracingService) {
            return ((ExecutionTracingService) service).isOperationExecuting(callerAddress, callerUuid, identifier);
        }
        logger.severe("Not able to find operation execution info. Invalid service: " + service);
        return false;
    }


    /**
     * Checks if an operation is still running.
     * <p/>
     * If the partition id is set, then it is super cheap since it just involves some volatiles reads since the right worker
     * thread can be found and in the worker-thread the current operation is stored in a volatile field.
     * <p/>
     * If the partition id isn't set, then we iterate over all generic-operationthread and check if one of them is running
     * the given operation. So this is a more expensive, but in most cases this should not be an issue since most of the data
     * is hot in cache.
     */
    @Override
    public boolean isOperationExecuting(Address callerAddress, int partitionId, long operationCallId) {
        if (partitionId < 0) {
            OperationRunner[] genericOperationRunners = operationExecutor.getGenericOperationRunners();
            for (OperationRunner genericOperationRunner : genericOperationRunners) {

                Object task = genericOperationRunner.currentTask();
                if (!(task instanceof Operation)) {
                    continue;
                }
                Operation op = (Operation) task;
                if (matches(op, callerAddress, operationCallId)) {
                    return true;
                }
            }
            return false;
        } else {
            OperationRunner[] partitionOperationRunners = operationExecutor.getPartitionOperationRunners();
            OperationRunner operationRunner = partitionOperationRunners[partitionId];
            Object task = operationRunner.currentTask();
            if (!(task instanceof Operation)) {
                return false;
            }
            Operation op = (Operation) task;
            return matches(op, callerAddress, operationCallId);
        }
    }

    private static boolean matches(Operation op, Address callerAddress, long operationCallId) {
        if (op == null) {
            return false;
        }

        if (op.getCallId() != operationCallId) {
            return false;
        }

        if (!op.getCallerAddress().equals(callerAddress)) {
            return false;
        }

        return true;
    }

    @Override
    public void onMemberLeft(final MemberImpl member) {
        // postpone notifying calls since real response may arrive in the mean time.
        nodeEngine.getExecutionService().schedule(new Runnable() {
            public void run() {
                final Iterator<BasicInvocation> iter = invocations.values().iterator();
                while (iter.hasNext()) {
                    final BasicInvocation invocation = iter.next();
                    if (invocation.isCallTarget(member)) {
                        iter.remove();
                        invocation.notify(new MemberLeftException(member));
                    }
                }
            }
        }, SCHEDULE_DELAY, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        shutdown = true;
        logger.finest("Stopping operation threads...");
        for (BasicInvocation invocation : invocations.values()) {
            try {
                invocation.notify(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage());
            }
        }
        invocations.clear();
        operationExecutor.shutdown();
    }

    /**
     * Executes an operation on a set of partitions.
     */
    private final class InvokeOnPartitions {

        public static final int TRY_COUNT = 10;
        public static final int TRY_PAUSE_MILLIS = 300;

        private final String serviceName;
        private final OperationFactory operationFactory;
        private final Map<Address, List<Integer>> memberPartitions;
        private final Map<Address, Future> futures;
        private final Map<Integer, Object> partitionResults;

        private InvokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                   Map<Address, List<Integer>> memberPartitions) {
            this.serviceName = serviceName;
            this.operationFactory = operationFactory;
            this.memberPartitions = memberPartitions;
            this.futures = new HashMap<Address, Future>(memberPartitions.size());
            this.partitionResults = new HashMap<Integer, Object>(nodeEngine.getPartitionService().getPartitionCount());
        }

        /**
         * Executes all the operations on the partitions.
         */
        private Map<Integer, Object> invoke() throws Exception {
            ensureNotCallingFromOperationThread();

            invokeOnAllPartitions();

            awaitCompletion();

            retryFailedPartitions();

            return partitionResults;
        }

        private void ensureNotCallingFromOperationThread() {
            if (operationExecutor.isOperationThread()) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
            }
        }

        private void invokeOnAllPartitions() {
            for (Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
                Address address = mp.getKey();
                List<Integer> partitions = mp.getValue();
                PartitionIteratingOperation pi = new PartitionIteratingOperation(partitions, operationFactory);
                Future future = createInvocationBuilder(serviceName, pi, address)
                        .setTryCount(TRY_COUNT)
                        .setTryPauseMillis(TRY_PAUSE_MILLIS)
                        .invoke();
                futures.put(address, future);
            }
        }

        private void awaitCompletion() {
            for (Map.Entry<Address, Future> response : futures.entrySet()) {
                try {
                    Future future = response.getValue();
                    PartitionResponse result = (PartitionResponse) nodeEngine.toObject(future.get());
                    partitionResults.putAll(result.asMap());
                } catch (Throwable t) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(t);
                    } else {
                        logger.warning(t.getMessage());
                    }
                    List<Integer> partitions = memberPartitions.get(response.getKey());
                    for (Integer partition : partitions) {
                        partitionResults.put(partition, t);
                    }
                }
            }
        }

        private void retryFailedPartitions() throws InterruptedException, ExecutionException {
            List<Integer> failedPartitions = new LinkedList<Integer>();
            for (Map.Entry<Integer, Object> partitionResult : partitionResults.entrySet()) {
                int partitionId = partitionResult.getKey();
                Object result = partitionResult.getValue();
                if (result instanceof Throwable) {
                    failedPartitions.add(partitionId);
                }
            }

            for (Integer failedPartition : failedPartitions) {
                Future f = createInvocationBuilder(serviceName, operationFactory.createOperation(), failedPartition).invoke();
                partitionResults.put(failedPartition, f);
            }

            for (Integer failedPartition : failedPartitions) {
                Future f = (Future) partitionResults.get(failedPartition);
                Object result = f.get();
                partitionResults.put(failedPartition, result);
            }
        }
    }

    /**
     * Responsible for handling responses.
     */
    private final class BasicResponsePacketHandler implements ResponsePacketHandler {

        @Override
        public Response deserialize(Packet packet) throws Exception {
            Data data = packet.getData();
            return (Response) nodeEngine.toObject(data);
        }

        @Override
        public void process(Response response) throws Exception {
            try {
                if (response instanceof NormalResponse || response instanceof CallTimeoutResponse) {
                    notifyRemoteCall(response);
                } else if (response instanceof BackupResponse) {
                    notifyBackupCall(response.getCallId());
                } else {
                    throw new IllegalStateException("Unrecognized response type: " + response);
                }
            } catch (Throwable e) {
                logger.severe("While processing response...", e);
            }
        }

        // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
        private void notifyRemoteCall(Response response) {
            BasicInvocation invocation = invocations.get(response.getCallId());
            if (invocation == null) {
                if (nodeEngine.isActive()) {
                    throw new HazelcastException("No invocation for response: " + response);
                }
                return;
            }

            invocation.notify(response);
        }
    }

    private class BasicOperationRunnerFactory implements OperationRunnerFactory {
        @Override
        public OperationRunner createAdHocRunner() {
            return new BasicOperationRunner(BasicOperationRunner.AD_HOC_PARTITION_ID);
        }

        @Override
        public OperationRunner createPartitionRunner(int partitionId) {
            return new BasicOperationRunner(partitionId);
        }

        @Override
        public OperationRunner createGenericRunner() {
            return new BasicOperationRunner(-1);
        }
    }

    /**
     * Responsible for processing an Operation.
     */
    private class BasicOperationRunner extends OperationRunner {
        private static final int AD_HOC_PARTITION_ID = -2;

        // This field doesn't need additional synchronization, since a partition-specific OperationRunner
        // will never be called concurrently.
        private InternalPartition internalPartition;

        // When partitionId >= 0, it is a partition specific
        // when partitionId =-1, it is generic
        // when partitionId =-2, it is ad hoc
        // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
        // and therefor the {@link OperationRunner#currentTask()} always returns null
        public BasicOperationRunner(int partitionId) {
            super(partitionId);
        }

        @Override
        public void run(Runnable task) {
            boolean publishCurrentTask = publishCurrentTask();

            if (publishCurrentTask) {
                currentTask = task;
            }

            try {
                task.run();
            } finally {
                if (publishCurrentTask) {
                    currentTask = null;
                }
            }
        }

        private boolean publishCurrentTask() {
            return getPartitionId() != AD_HOC_PARTITION_ID && currentTask == null;
        }

        @Override
        public void run(Operation op) {
            // TODO: We need to think about replacing this contended counter.
            executedOperationsCount.incrementAndGet();

            boolean publishCurrentTask = publishCurrentTask();

            if (publishCurrentTask) {
                currentTask = op;
            }

            try {
                if (timeout(op)) {
                    return;
                }

                ensureNoPartitionProblems(op);

                op.beforeRun();

                if (waitingNeeded(op)) {
                    return;
                }

                op.run();
                handleResponse(op);
                afterRun(op);
            } catch (Throwable e) {
                handleOperationError(op, e);
            } finally {
                if (publishCurrentTask) {
                    currentTask = null;
                }
            }
        }

        private boolean waitingNeeded(Operation op) {
            if (op instanceof WaitSupport) {
                WaitSupport waitSupport = (WaitSupport) op;
                if (waitSupport.shouldWait()) {
                    nodeEngine.waitNotifyService.await(waitSupport);
                    return true;
                }
            }
            return false;
        }

        private boolean timeout(Operation op) {
            if (isCallTimedOut(op)) {
                op.getResponseHandler().sendResponse(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
                return true;
            }
            return false;
        }

        private void handleResponse(Operation op) throws Exception {
            boolean returnsResponse = op.returnsResponse();
            Object response = null;
            if (op instanceof BackupAwareOperation) {
                BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
                int syncBackupCount = 0;
                if (backupAwareOp.shouldBackup()) {
                    syncBackupCount = operationBackupHandler.backup(backupAwareOp);
                }
                if (returnsResponse) {
                    response = new NormalResponse(op.getResponse(), op.getCallId(), syncBackupCount, op.isUrgent());
                }
            }

            if (returnsResponse) {
                if (response == null) {
                    response = op.getResponse();
                }
                com.hazelcast.spi.ResponseHandler responseHandler = op.getResponseHandler();
                if (responseHandler == null) {
                    throw new IllegalStateException("ResponseHandler should not be null!");
                }
                responseHandler.sendResponse(response);
            }
        }

        private void afterRun(Operation op) {
            try {
                op.afterRun();
                if (op instanceof Notifier) {
                    final Notifier notifier = (Notifier) op;
                    if (notifier.shouldNotify()) {
                        nodeEngine.waitNotifyService.notify(notifier);
                    }
                }
            } catch (Throwable e) {
                // passed the response phase
                // `afterRun` and `notifier` errors cannot be sent to the caller anymore
                // just log the error
                logOperationError(op, e);
            }
        }

        protected void ensureNoPartitionProblems(Operation op) {
            int partitionId = op.getPartitionId();

            if (partitionId < 0) {
                return;
            }

            if (partitionId != getPartitionId()) {
                throw new IllegalStateException("wrong partition, expected: " + getPartitionId() + " but found:" + op);
            }

            if (internalPartition == null) {
                internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
            }

            if (retryDuringMigration(op) && internalPartition.isMigrating()) {
                throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                        op.getClass().getName(), op.getServiceName());
            }

            Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
            if (op.validatesTarget() && !node.getThisAddress().equals(owner)) {
                throw new WrongTargetException(node.getThisAddress(), owner, partitionId, op.getReplicaIndex(),
                        op.getClass().getName(), op.getServiceName());
            }
        }

        private boolean retryDuringMigration(Operation op) {
            return !(op instanceof ReadonlyOperation || OperationAccessor.isMigrationOperation(op));
        }

        private void handleOperationError(Operation operation, Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            operation.logError(e);
            com.hazelcast.spi.ResponseHandler responseHandler = operation.getResponseHandler();
            if (operation.returnsResponse() && responseHandler != null) {
                try {
                    if (node.isActive()) {
                        responseHandler.sendResponse(e);
                    } else if (responseHandler.isLocal()) {
                        responseHandler.sendResponse(new HazelcastInstanceNotActiveException());
                    }
                } catch (Throwable t) {
                    logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
                }
            }
        }

        private void logOperationError(Operation op, Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            op.logError(e);
        }

        @Override
        public void run(Packet packet) throws Exception {
            boolean publishCurrentTask = publishCurrentTask();

            if (publishCurrentTask) {
                currentTask = packet;
            }

            Connection connection = packet.getConn();
            Address caller = connection.getEndPoint();
            Data data = packet.getData();
            try {
                Object object = nodeEngine.toObject(data);
                Operation op = (Operation) object;
                op.setNodeEngine(nodeEngine);
                setCallerAddress(op, caller);
                setConnection(op, connection);
                setCallerUuidIfNotSet(caller, op);
                setRemoteResponseHandler(nodeEngine, op);

                if (!ensureValidMember(op)) {
                    return;
                }

                if (publishCurrentTask) {
                    currentTask = null;
                }
                run(op);
            } catch (Throwable throwable) {
                // If exception happens we need to extract the callId from the bytes directly!
                long callId = IOUtil.extractOperationCallId(data, node.getSerializationService());
                send(new NormalResponse(throwable, callId, 0, packet.isUrgent()), caller);
                logOperationDeserializationException(throwable, callId);
                throw ExceptionUtil.rethrow(throwable);
            } finally {
                if (publishCurrentTask) {
                    currentTask = null;
                }
            }
        }

        private boolean ensureValidMember(Operation op) {
            if (!isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                        op.getClass().getName(), op.getServiceName());
                handleOperationError(op, error);
                return false;
            }
            return true;
        }

        private void setCallerUuidIfNotSet(Address caller, Operation op) {
            if (op.getCallerUuid() != null) {
                return;

            }
            MemberImpl callerMember = node.clusterService.getMember(caller);
            if (callerMember != null) {
                op.setCallerUuid(callerMember.getUuid());
            }
        }

        public void logOperationDeserializationException(Throwable t, long callId) {
            boolean returnsResponse = callId != 0;

            if (t instanceof RetryableException) {
                final Level level = returnsResponse ? Level.FINEST : Level.WARNING;
                if (logger.isLoggable(level)) {
                    logger.log(level, t.getClass().getName() + ": " + t.getMessage());
                }
            } else if (t instanceof OutOfMemoryError) {
                try {
                    logger.log(Level.SEVERE, t.getMessage(), t);
                } catch (Throwable ignored) {
                    logger.log(Level.SEVERE, ignored.getMessage(), t);
                }
            } else {
                final Level level = nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
                if (logger.isLoggable(level)) {
                    logger.log(level, t.getMessage(), t);
                }
            }
        }
    }

    /**
     * Responsible for creating a backups of an operation.
     */
    private final class OperationBackupHandler {

        public int backup(BackupAwareOperation backupAwareOp) throws Exception {
            int requestedSyncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                    ? min(InternalPartition.MAX_BACKUP_COUNT, backupAwareOp.getSyncBackupCount()) : 0;

            int requestedAsyncBackupCount = backupAwareOp.getAsyncBackupCount() > 0
                    ? min(InternalPartition.MAX_BACKUP_COUNT - requestedSyncBackupCount,
                    backupAwareOp.getAsyncBackupCount()) : 0;

            int totalRequestedBackupCount = requestedSyncBackupCount + requestedAsyncBackupCount;
            if (totalRequestedBackupCount == 0) {
                return 0;
            }

            Operation op = (Operation) backupAwareOp;
            InternalPartitionService partitionService = node.getPartitionService();
            long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(op.getPartitionId(),
                    totalRequestedBackupCount);

            int maxPossibleBackupCount = partitionService.getMaxBackupCount();
            int syncBackupCount = min(maxPossibleBackupCount, requestedSyncBackupCount);
            int asyncBackupCount = min(maxPossibleBackupCount - syncBackupCount, requestedAsyncBackupCount);

            int totalBackupCount = syncBackupCount + asyncBackupCount;
            if (totalBackupCount == 0) {
                return 0;
            }

            if (!op.returnsResponse()) {
                syncBackupCount = 0;
            }

            return makeBackups(backupAwareOp, op.getPartitionId(), replicaVersions, syncBackupCount, totalBackupCount);
        }

        private int makeBackups(BackupAwareOperation backupAwareOp, int partitionId, long[] replicaVersions,
                                int syncBackupCount, int totalBackupCount) {
            Boolean backPressureNeeded = null;
            int sentSyncBackupCount = 0;
            InternalPartitionService partitionService = node.getPartitionService();
            InternalPartition partition = partitionService.getPartition(partitionId);
            for (int replicaIndex = 1; replicaIndex <= totalBackupCount; replicaIndex++) {
                Address target = partition.getReplicaAddress(replicaIndex);
                if (target == null) {
                    continue;
                }

                assertNoBackupOnPrimaryMember(partition, target);

                boolean isSyncBackup = true;
                if (replicaIndex > syncBackupCount) {
                    // it is an async-backup

                    if (backPressureNeeded == null) {
                        // back-pressure was not yet calculated, so lets calculate it. Once it is calculated
                        // we'll use that value for all the backups for the 'backupAwareOp'.
                        backPressureNeeded = backPressureService.isBackPressureNeeded((Operation) backupAwareOp);
                    }

                    if (!backPressureNeeded) {
                        // only when no back-pressure is needed, then the async-backup is allowed to be async.
                        isSyncBackup = false;
                    }
                }

                Backup backup = newBackup(backupAwareOp, replicaVersions, replicaIndex, isSyncBackup);
                send(backup, target);

                if (isSyncBackup) {
                    sentSyncBackupCount++;
                }
            }
            return sentSyncBackupCount;
        }

        private Backup newBackup(BackupAwareOperation backupAwareOp, long[] replicaVersions,
                                 int replicaIndex, boolean isSyncBackup) {
            Operation op = (Operation) backupAwareOp;
            Operation backupOp = initBackupOperation(backupAwareOp, replicaIndex);
            Data backupOpData = nodeEngine.getSerializationService().toData(backupOp);
            Backup backup = new Backup(backupOpData, op.getCallerAddress(), replicaVersions, isSyncBackup);
            backup.setPartitionId(op.getPartitionId())
                    .setReplicaIndex(replicaIndex)
                    .setServiceName(op.getServiceName())
                    .setCallerUuid(nodeEngine.getLocalMember().getUuid());
            setCallId(backup, op.getCallId());
            return backup;
        }

        private Operation initBackupOperation(BackupAwareOperation backupAwareOp, int replicaIndex) {
            Operation backupOp = backupAwareOp.getBackupOperation();
            if (backupOp == null) {
                throw new IllegalArgumentException("Backup operation should not be null!");
            }

            Operation op = (Operation) backupAwareOp;
            backupOp.setPartitionId(op.getPartitionId())
                    .setReplicaIndex(replicaIndex)
                    .setServiceName(op.getServiceName());
            return backupOp;
        }

        /**
         * Verifies that the backup of a partition doesn't end up at the member that also has the primary.
         */
        private void assertNoBackupOnPrimaryMember(InternalPartition partition, Address target) {
            if (target.equals(node.getThisAddress())) {
                throw new IllegalStateException("Normally shouldn't happen! Owner node and backup node "
                        + "are the same! " + partition);
            }
        }
    }

    /**
     * The CleanupThread does 2 things:
     * - deals with operations that need to be re-invoked.
     * - deals with cleanup of the BackPressureService.
     *
     * It periodically iterates over all invocations in this BasicOperationService and calls the
     * {@link BasicInvocation#handleOperationTimeout()} {@link BasicInvocation#handleBackupTimeout(long)} methods.
     * This gives each invocation the opportunity to handle with an operation (especially required for async ones)
     * and/or a backup not completing in time.
     * <p/>
     * The previous approach was that for each BackupAwareOperation a task was scheduled to deal with the timeout. The problem
     * is that the actual operation already could be completed, but the task is still scheduled and this can lead to an OOME.
     * Apart from that it also had quite an impact on performance since there is more interaction with concurrent data-structures
     * (e.g. the priority-queue of the scheduled-executor).
     * <p/>
     * We use a dedicated thread instead of a shared ScheduledThreadPool because there will not be that many of these threads
     * (each member-HazelcastInstance gets 1) and we don't want problems in 1 member causing problems in the other.
     */
    private final class CleanupThread extends Thread {

        public static final int DELAY_MILLIS = 1000;

        private CleanupThread() {
            super(node.getHazelcastThreadGroup().getThreadNamePrefix("CleanupThread"));
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    scanHandleOperationTimeout();
                    backPressureService.cleanup();
                    sleep();
                }

            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to run", t);
            }
        }

        private void sleep() {
            try {
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException ignore) {
                // can safely be ignored. If this thread wants to shut down, we'll read the shutdown variable.
                EmptyStatement.ignore(ignore);
            }
        }

        private void scanHandleOperationTimeout() {
            if (invocations.isEmpty()) {
                return;
            }

            for (BasicInvocation invocation : invocations.values()) {
                try {
                    invocation.handleOperationTimeout();
                } catch (Throwable t) {
                    inspectOutputMemoryError(t);
                    logger.severe("Failed to handle operation timeout of invocation:" + invocation, t);
                }
                try {
                    invocation.handleBackupTimeout(backupOperationTimeoutMillis);
                } catch (Throwable t) {
                    inspectOutputMemoryError(t);
                    logger.severe("Failed to handle backup timeout of invocation:" + invocation, t);
                }
            }
        }
    }

}

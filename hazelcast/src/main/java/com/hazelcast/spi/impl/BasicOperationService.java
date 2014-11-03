/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.BackoffPolicy;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ExecutionTracingService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.WriteResult;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.Clock;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.OperationAccessor.isJoinOperation;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setConnection;
import static com.hazelcast.spi.OperationAccessor.setStartTime;
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

    //todo: a temporary hack for the back pressure since icompletablefuture completion handler is going to be executed
    //on this thread.
    private static final int ASYNC_QUEUE_CAPACITY = Integer.MAX_VALUE;

    final ConcurrentMap<Long, BasicInvocation> invocations;
    final BasicOperationScheduler scheduler;
    final ILogger invocationLogger;
    final ManagedExecutorService asyncExecutor;
    private final AtomicLong executedOperationsCount = new AtomicLong();

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong callIdGen = new AtomicLong(1);

    private final Map<RemoteCallKey, RemoteCallKey> executingCalls;

    private final long defaultCallTimeoutMillis;
    private final long backupOperationTimeoutMillis;
    private final ExecutionService executionService;
    private final OperationHandler operationHandler;
    private final OperationBackupHandler operationBackupHandler;
    private final OperationPacketHandler operationPacketHandler;
    private final ResponsePacketHandler responsePacketHandler;
    private final BackupTimeoutHandlerThread backupTimeoutHandlerThread;
    private volatile boolean shutdown;

    private final BackoffPolicy backoffPolicy;

    BasicOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class);
        this.invocationLogger = nodeEngine.getLogger(BasicInvocation.class);
        this.defaultCallTimeoutMillis = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        this.backupOperationTimeoutMillis = node.getGroupProperties().OPERATION_BACKUP_TIMEOUT_MILLIS.getLong();

        this.executionService = nodeEngine.getExecutionService();

        int coreSize = Runtime.getRuntime().availableProcessors();
        boolean reallyMultiCore = coreSize >= CORE_SIZE_CHECK;
        int concurrencyLevel = reallyMultiCore ? coreSize * CORE_SIZE_FACTOR : CONCURRENCY_LEVEL;
        this.executingCalls =
                new ConcurrentHashMap<RemoteCallKey, RemoteCallKey>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);
        this.invocations = new ConcurrentHashMap<Long, BasicInvocation>(INITIAL_CAPACITY, LOAD_FACTOR, concurrencyLevel);
        this.scheduler = new BasicOperationScheduler(node, executionService, new BasicDispatcherImpl());
        this.operationHandler = new OperationHandler();
        this.operationBackupHandler = new OperationBackupHandler();
        this.operationPacketHandler = new OperationPacketHandler();
        this.responsePacketHandler = new ResponsePacketHandler();

        this.backoffPolicy = new ExponentialBackoffPolicy();

        executionService.register(ExecutionService.ASYNC_EXECUTOR, coreSize,
                ASYNC_QUEUE_CAPACITY, ExecutorType.CONCRETE);

        this.backupTimeoutHandlerThread = new BackupTimeoutHandlerThread();
        this.backupTimeoutHandlerThread.start();
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
        scheduler.dumpPerformanceMetrics(sb);
    }

    @Override
    public int getPartitionOperationThreadCount() {
        return scheduler.partitionOperationThreads.length;
    }

    @Override
    public int getGenericOperationThreadCount() {
        return scheduler.genericOperationThreads.length;
    }

    @Override
    public int getRunningOperationsCount() {
        return executingCalls.size();
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
        return 0;
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return scheduler.getOperationExecutorQueueSize();
    }

    @Override
    public int getPriorityOperationExecutorQueueSize() {
        return scheduler.getPriorityOperationExecutorQueueSize();
    }

    @Override
    public void execute(Runnable task, int partitionId) {
        scheduler.execute(task, partitionId);
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

    @PrivateApi
    @Override
    public void executeOperation(final Packet packet) {
        scheduler.execute(packet);
    }

    /**
     * Runs operation in calling thread.
     *
     * @param op
     */
    @Override
    //todo: move to BasicOperationScheduler
    public void runOperationOnCallingThread(Operation op) {
        if (scheduler.isAllowedToRunInCurrentThread(op)) {
            operationHandler.handle(op);
        } else {
            throw new IllegalThreadStateException("Operation: " + op + " cannot be run in current thread! -> "
                    + Thread.currentThread());
        }
    }

    @Override
    public void executeOperation(final Operation op) {
        scheduler.execute(op);
    }

    @Override
    public boolean isAllowedToRunOnCallingThread(Operation op) {
        return scheduler.isAllowedToRunInCurrentThread(op);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        return new BasicPartitionInvocation(nodeEngine, serviceName, op, partitionId, InvocationBuilder.DEFAULT_REPLICA_INDEX,
                InvocationBuilder.DEFAULT_TRY_COUNT, InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                InvocationBuilder.DEFAULT_CALL_TIMEOUT, null, null, InvocationBuilder.DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
        return new BasicTargetInvocation(nodeEngine, serviceName, op, target, InvocationBuilder.DEFAULT_TRY_COUNT,
                InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                InvocationBuilder.DEFAULT_CALL_TIMEOUT, null, null, InvocationBuilder.DEFAULT_DESERIALIZE_RESULT).invoke();
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

    @PrivateApi
    public boolean isCallTimedOut(Operation op) {
        if (op.returnsResponse() && op.getCallId() != 0) {
            final long callTimeout = op.getCallTimeout();
            final long invocationTime = op.getInvocationTime();
            final long expireTime = invocationTime + callTimeout;
            if (expireTime > 0 && expireTime < Long.MAX_VALUE) {
                final long now = nodeEngine.getClusterTime();
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

        // todo: a hack to prevent urgent backup operations be kicked out of this method.
        if (op instanceof BackupOperation && !op.isUrgent()) {
            System.out.println("Unexpected operation: " + op);
            //throw new IllegalArgumentException("Can't send a Backup operation using the regular send method, op: " + op);
        }

        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }
        Data data = nodeEngine.toData(op);
        int partitionId = scheduler.getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);

        if (op instanceof UrgentSystemOperation) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        Connection connection = node.getConnectionManager().getOrConnect(target);

        WriteResult sent = nodeEngine.send(packet, connection);
        if (sent != WriteResult.FULL) {
            return sent == WriteResult.SUCCESS;
        }

        int maxAttempts = 1000; //TODO: Refactor it to GroupProperties
        int state = BackoffPolicy.EMPTY_STATE;
        for (int i = 0; i < maxAttempts; i++) {
            state = backoffPolicy.apply(state);
            sent = nodeEngine.send(packet, connection);
            if (sent != WriteResult.FULL) {
                return sent == WriteResult.SUCCESS;
            }
        }
        return false;
    }

    /**
     * Writes the backup. The backup is ALWAYS going to be accepted by the connection because we can't apply back-pressure
     * on the partition thread. But this method will return if the connection had enough capacity
     *
     * @param op
     * @param target
     * @return
     */
    private WriteResult sendBackup(Operation op, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }

        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }

        if (!(op instanceof BackupOperation)) {
            throw new IllegalArgumentException("Only BackupOperations are allowed to be executed using the sendBackup method. " +
                    "Operation = " + op);
        }

        Data data = nodeEngine.toData(op);
        int partitionId = scheduler.getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);

        if (op instanceof UrgentSystemOperation) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        Connection connection = node.getConnectionManager().getOrConnect(target);
        return nodeEngine.sendBackup(packet, connection);
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
        return nodeEngine.send(packet, connection) == WriteResult.SUCCESS;
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
        invocations.remove(invocation.op.getCallId());
    }

    @PrivateApi
    long getDefaultCallTimeoutMillis() {
        return defaultCallTimeoutMillis;
    }

    @PrivateApi
    boolean isOperationExecuting(Address callerAddress, String callerUuid, long operationCallId) {
        return executingCalls.containsKey(new RemoteCallKey(callerAddress, operationCallId));
    }

    @PrivateApi
    boolean isOperationExecuting(Address callerAddress, String callerUuid, String serviceName, Object identifier) {
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
        final Object response = new HazelcastInstanceNotActiveException();
        for (BasicInvocation invocation : invocations.values()) {
            try {
                invocation.notify(response);
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage());
            }
        }
        invocations.clear();
        scheduler.shutdown();
    }

    @Override
    public int getNoOfScheduledOperations() {
        return scheduler.getNoOfScheduledOperations();
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
            Thread currentThread = Thread.currentThread();
            if (currentThread instanceof BasicOperationScheduler.OperationThread) {
                throw new IllegalThreadStateException(currentThread + " cannot make invocation on multiple partitions!");
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

    public final class BasicDispatcherImpl implements BasicDispatcher {

        @Override
        public void dispatch(Object task) {
            if (task == null) {
                throw new IllegalArgumentException();
            }

            if (task instanceof Operation) {
                operationHandler.handle((Operation) task);
            } else if (task instanceof Packet) {
                Packet packet = (Packet) task;
                if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                    responsePacketHandler.handle(packet);
                } else {
                    operationPacketHandler.handle(packet);
                }
            } else if (task instanceof Runnable) {
                ((Runnable) task).run();
            } else {
                throw new IllegalArgumentException("Unrecognized task:" + task);
            }
        }
    }

    /**
     * Responsible for handling responses.
     */
    private final class ResponsePacketHandler {
        private void handle(Packet packet) {
            try {
                Data data = packet.getData();
                Response response = (Response) nodeEngine.toObject(data);

                if (response instanceof NormalResponse) {
                    notifyRemoteCall((NormalResponse) response);
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
        private void notifyRemoteCall(NormalResponse response) {
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

    /**
     * Responsible for handling operation packets.
     */
    private final class OperationPacketHandler {

        /**
         * Handles this packet.
         */
        private void handle(Packet packet) {
            try {
                Operation op = loadOperation(packet);

                if (!ensureValidMember(op)) {
                    return;
                }

                handle(op);
            } catch (Throwable e) {
                logger.severe(e);
            }
        }

        private Operation loadOperation(Packet packet) throws Exception {
            Connection conn = packet.getConn();
            Address caller = conn.getEndPoint();
            Data data = packet.getData();
            try {
                Object object = nodeEngine.toObject(data);
                Operation op = (Operation) object;
                op.setNodeEngine(nodeEngine);
                setCallerAddress(op, caller);
                setConnection(op, conn);
                setCallerUuidIfNotSet(caller, op);
                setRemoteResponseHandler(nodeEngine, op);
                return op;
            } catch (Throwable throwable) {
                // If exception happens we need to extract the callId from the bytes directly!
                long callId = IOUtil.extractOperationCallId(data, node.getSerializationService());
                RemoteOperationExceptionHandler exceptionHandler = new RemoteOperationExceptionHandler(callId);
                exceptionHandler.setNodeEngine(nodeEngine);
                exceptionHandler.setCallerAddress(caller);
                exceptionHandler.setConnection(conn);
                ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, exceptionHandler);
                operationHandler.handleOperationError(exceptionHandler, throwable);
                throw ExceptionUtil.rethrow(throwable);
            }
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

        private boolean ensureValidMember(Operation op) {
            if (!isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                        op.getClass().getName(), op.getServiceName());
                operationHandler.handleOperationError(op, error);
                return false;
            }
            return true;
        }

        private void handle(Operation op) {
            String executorName = op.getExecutorName();
            if (executorName == null) {
                operationHandler.handle(op);
            } else {
                offloadOperationHandling(op);
            }
        }

        private void offloadOperationHandling(final Operation op) {
            String executorName = op.getExecutorName();

            ExecutorService executor = executionService.getExecutor(executorName);
            if (executor == null) {
                throw new IllegalStateException("Could not found executor with name: " + executorName);
            }

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    operationHandler.handle(op);
                }
            });
        }
    }

    /**
     * Responsible for processing an Operation.
     */
    private final class OperationHandler {

        /**
         * Runs operation in calling thread.
         */
        private void handle(Operation op) {
            executedOperationsCount.incrementAndGet();

            RemoteCallKey callKey = null;
            try {
                if (timeout(op)) {
                    return;
                }

                callKey = beforeCallExecution(op);

                ensureNoPartitionProblems(op);

                beforeRun(op);

                if (waitingNeeded(op)) {
                    return;
                }

                op.run();
                handleResponse(op);
                afterRun(op);
            } catch (Throwable e) {
                handleOperationError(op, e);
            } finally {
                afterCallExecution(op, callKey);
            }
        }

        private void beforeRun(Operation op) throws Exception {
            setStartTime(op, Clock.currentTimeMillis());
            op.beforeRun();
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
                Object response = new CallTimeoutException(
                        op.getClass().getName(), op.getInvocationTime(), op.getCallTimeout());
                op.getResponseHandler().sendResponse(response);
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
                ResponseHandler responseHandler = op.getResponseHandler();
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

        private void ensureNoPartitionProblems(Operation op) {
            if (!(op instanceof PartitionAwareOperation)) {
                return;
            }

            int partitionId = op.getPartitionId();
            if (partitionId < 0) {
                throw new IllegalArgumentException("Partition id cannot be negative! -> " + partitionId);
            }

            InternalPartition internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
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

        private RemoteCallKey beforeCallExecution(Operation op) {
            RemoteCallKey callKey = null;
            if (op.getCallId() != 0 && op.returnsResponse()) {
                callKey = new RemoteCallKey(op);
                RemoteCallKey current = executingCalls.put(callKey, callKey);
                if (current != null) {
                    logger.warning("Duplicate Call record! -> " + callKey + " / " + current + " == " + op.getClass().getName());
                }
            }
            return callKey;
        }

        private void afterCallExecution(Operation op, RemoteCallKey callKey) {
            if (callKey != null && op.getCallId() != 0 && op.returnsResponse()) {
                if (executingCalls.remove(callKey) == null) {
                    logger.severe("No Call record has been found: -> " + callKey + " == " + op.getClass().getName());
                }
            }
        }

        private void handleOperationError(RemotePropagatable remotePropagatable, Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            remotePropagatable.logError(e);
            ResponseHandler responseHandler = remotePropagatable.getResponseHandler();
            if (remotePropagatable.returnsResponse() && responseHandler != null) {
                try {
                    if (node.isActive()) {
                        responseHandler.sendResponse(e);
                    } else if (responseHandler.isLocal()) {
                        responseHandler.sendResponse(new HazelcastInstanceNotActiveException());
                    }
                } catch (Throwable t) {
                    logger.warning("While sending op error... op: " + remotePropagatable + ", error: " + e, t);
                }
            }
        }

        private void logOperationError(Operation op, Throwable e) {
            if (e instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            }
            op.logError(e);
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

        // just for debugging.
        private final AtomicLong backupCounter = new AtomicLong();
        private final AtomicLong fullCOunter = new AtomicLong();
        private final AtomicLong notFullCounter = new AtomicLong();

        /**
         * Makes the actual backup.
         *
         * @param backupAwareOp
         * @param partitionId
         * @param replicaVersions
         * @param desiredSyncBackups
         * @param totalBackupCount
         * @return the number of sync backups. If one of the connections is full, the number of sync backups is not determined
         * based on the desiredSyncBackups, but by the total number of backups done. Because the future is going to wait for every
         * backup to complete
         */
        private int makeBackups(BackupAwareOperation backupAwareOp, int partitionId, long[] replicaVersions,
                                int desiredSyncBackups, int totalBackupCount) {

            int syncBackups = 0;
            int asyncBackups = 0;

            boolean fullConnectionEncountered = false;
            InternalPartitionService partitionService = node.getPartitionService();
            InternalPartition partition = partitionService.getPartition(partitionId);

            // bug:
            // assuming a single async backup. So this operation is created and the Backup (operation) is marked as async since
            // that is what you want. When the backup is send to the connection, the connection figures out that it is full
            // and eventually the future will be notified that it needs to wait for one backup. The problem is that the backup
            // was configured as sync, and therefor will never contact that future.
            // fix:
            // we should check the connection before we are going to send backup. If the connection is full, the backup and the
            // future are now configured as sync. This way the backup and the future are always configured the same.
            for (int replicaIndex = 1; replicaIndex <= totalBackupCount; replicaIndex++) {
                Address target = partition.getReplicaAddress(replicaIndex);
                if (target == null) {
                    continue;
                }

                assertNoBackupOnPrimaryMember(partition, target);

                boolean isSyncBackup = replicaIndex <= desiredSyncBackups;
                if (!isSyncBackup) {
                    Connection connection = node.getConnectionManager().getOrConnect(target);
                    if(connection.isFull()){
                        //isSyncBackup = true;
                        fullConnectionEncountered = true;
                    }
                }
                Backup backup = newBackup(backupAwareOp, replicaVersions, replicaIndex, isSyncBackup);

                WriteResult result = sendBackup(backup, target);
//
//                if (result == WriteResult.FULL) {
//                    fullConnectionEncountered = true;
//                    if (lastFullTimeMs + 10000 < System.currentTimeMillis()) {
//                        lastFullTimeMs = System.currentTimeMillis();
//                        logger.severe("Back pressure applied on async backup calls");
//                    }
//                }

                if (isSyncBackup) {
                    syncBackups++;
                } else {
                    asyncBackups++;
                }
            }

            if (fullConnectionEncountered) {
                fullCOunter.incrementAndGet();
            } else {
                notFullCounter.incrementAndGet();
            }

            if ((backupCounter.incrementAndGet() % 20) == 0) {
                logger.info(backupCounter.get() + "  Backups sync = " + syncBackups + " fullCounter: " + fullCOunter + " notFullCounter: " + notFullCounter);
            }

            logger.severe("syncBackup:" + syncBackups);

            return syncBackups;
        }

        private volatile long lastFullTimeMs = System.currentTimeMillis();

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

    private static final class RemoteCallKey {
        private final long time = Clock.currentTimeMillis();
        private final Address callerAddress;
        private final long callId;

        private RemoteCallKey(Address callerAddress, long callId) {
            if (callerAddress == null) {
                throw new IllegalArgumentException("Caller address is required!");
            }
            this.callerAddress = callerAddress;
            this.callId = callId;
        }

        private RemoteCallKey(final Operation op) {
            callerAddress = op.getCallerAddress();
            if (callerAddress == null) {
                throw new IllegalArgumentException("Caller address is required! -> " + op);
            }
            callId = op.getCallId();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RemoteCallKey callKey = (RemoteCallKey) o;
            if (callId != callKey.callId) {
                return false;
            }
            if (!callerAddress.equals(callKey.callerAddress)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = callerAddress.hashCode();
            result = 31 * result + (int) (callId ^ (callId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("RemoteCallKey");
            sb.append("{callerAddress=").append(callerAddress);
            sb.append(", callId=").append(callId);
            sb.append(", time=").append(time);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * The HandleBackupTimeoutThread periodically iterates over all invocations in this BasicOperationService and calls the
     * {@link BasicInvocation#handleBackupTimeout(long)} method. This gives each invocation the opportunity to handle with a
     * backup not completing in time.
     * <p/>
     * The previous approach was that for each BackupAwareOperation a task was scheduled to deal with the timeout. The problem
     * is that the actual operation already could be completed, but the task is still scheduled and this can lead to an OOME.
     * Apart from that it also had quite an impact on performance since there is more interaction with concurrent data-structures
     * (e.g. the priority-queue of the scheduled-executor).
     * <p/>
     * We use a dedicates thread instead of a shared ScheduledThreadPool because there will not be that many of these threads
     * (each member-HazelcastInstance gets 1) and we don't want problems in 1 member causing problems in the other.
     */
    private final class BackupTimeoutHandlerThread extends Thread {

        public static final int DELAY_MILLIS = 1000;

        private BackupTimeoutHandlerThread() {
            super(node.getThreadNamePrefix("BackupTimeoutHandlerThread"));
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    scan();

                    try {
                        Thread.sleep(DELAY_MILLIS);
                    } catch (InterruptedException ignore) {
                        // can safely be ignored. If this thread wants to shut down, we'll read the shutdown variable.
                        EmptyStatement.ignore(ignore);
                    }
                }
            } catch (Throwable t) {
                logger.severe("Failed to run", t);
            }
        }

        private void scan() {
            if (invocations.isEmpty()) {
                return;
            }

            for (BasicInvocation invocation : invocations.values()) {
                try {
                    invocation.handleBackupTimeout(backupOperationTimeoutMillis);
                } catch (Throwable t) {
                    logger.severe("Failed to handle backup timeout of invocation:" + invocation, t);
                }
            }
        }
    }
}

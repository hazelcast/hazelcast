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
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupCompletionCallback;
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
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.OperationAccessor.isJoinOperation;

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

    private final AtomicLong executedOperationsCount = new AtomicLong();

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong callIdGen = new AtomicLong(0);
    final ConcurrentMap<Long, RemoteCall> remoteCalls;

    private final ExecutorService responseExecutor;
    private final long defaultCallTimeout;
    private final Map<RemoteCallKey, RemoteCallKey> executingCalls;
    final ConcurrentMap<Long, BackupCompletionCallback> backupCalls;
    private final int operationThreadCount;
    private final BlockingQueue<Runnable> responseWorkQueue = new LinkedBlockingQueue<Runnable>();
    private final ExecutionService executionService;
    private final BasicOperationScheduler executor;

    BasicOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class);
        defaultCallTimeout = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        int coreSize = Runtime.getRuntime().availableProcessors();
        boolean reallyMultiCore = coreSize >= 8;
        int concurrencyLevel = reallyMultiCore ? coreSize * 4 : 16;
        remoteCalls = new ConcurrentHashMap<Long, RemoteCall>(1000, 0.75f, concurrencyLevel);
        int opThreadCount = node.getGroupProperties().OPERATION_THREAD_COUNT.getInteger();
        operationThreadCount = opThreadCount > 0 ? opThreadCount : coreSize * 2;

        executionService = nodeEngine.getExecutionService();
        executionService.register(ExecutionService.ASYNC_EXECUTOR, coreSize * 5, coreSize * 100000,
                ExecutorType.CONCRETE);

        responseExecutor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                responseWorkQueue,
                new SingleExecutorThreadFactory(node.threadGroup,
                        node.getConfigClassLoader(), node.getThreadNamePrefix("response")));

        executingCalls = new ConcurrentHashMap<RemoteCallKey, RemoteCallKey>(1000, 0.75f, concurrencyLevel);
        backupCalls = new ConcurrentHashMap<Long, BackupCompletionCallback>(1000, 0.75f, concurrencyLevel);

        this.executor = new BasicOperationScheduler(
                node, executionService, operationThreadCount, new BasicOperationProcessorImpl());
    }

    @Override
    public int getOperationThreadCount() {
        return operationThreadCount;
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
        return remoteCalls.size();
    }

    @Override
    public int getResponseQueueSize() {
        return responseWorkQueue.size();
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return executor.getOperationExecutorQueueSize();
    }

    @Override
    public int getPriorityOperationExecutorQueueSize() {
        return executor.getPriorityOperationExecutorQueueSize();
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("Partition id cannot be negative!");
        return new BasicInvocationBuilder(nodeEngine, serviceName, op, partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        if (target == null) throw new IllegalArgumentException("Target cannot be null!");
        return new BasicInvocationBuilder(nodeEngine, serviceName, op, target);
    }

    @PrivateApi
    @Override
    public void receive(final Packet packet) {
        try {
            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                responseExecutor.execute(new ResponseProcessor(packet));
            } else {
                int partitionId = packet.getPartitionId();
                boolean systemOperation = packet.isUrgent();
                executor.execute(packet, partitionId, systemOperation);
            }
        } catch (RejectedExecutionException e) {
            if (nodeEngine.isActive()) {
                throw e;
            }
        }
    }

    private int getPartitionIdForExecution(Operation op) {
        return op instanceof PartitionAwareOperation ? op.getPartitionId() : -1;
    }

    /**
     * Runs operation in calling thread.
     *
     * @param op
     */
    @Override
    //todo: move to BasicOperationScheduler
    public void runOperation(Operation op) {
        if (isAllowedToRunInCurrentThread(op)) {
            processOperation(op);
        } else {
            throw new IllegalThreadStateException("Operation: " + op + " cannot be run in current thread! -> " +
                    Thread.currentThread());
        }
    }

    //todo: move to BasicOperationScheduler
    boolean isAllowedToRunInCurrentThread(Operation op) {
        return executor.isAllowedToRunInCurrentThread(getPartitionIdForExecution(op));
    }

    //todo: move to BasicOperationScheduler
    boolean isInvocationAllowedFromCurrentThread(Operation op) {
        return executor.isInvocationAllowedFromCurrentThread(getPartitionIdForExecution(op));
    }

    /**
     * Executes operation in operation executor pool.
     *
     * @param op
     */
    @Override
    public void executeOperation(final Operation op) {
        String executorName = op.getExecutorName();
        if (executorName == null) {
            int partitionId = getPartitionIdForExecution(op);
            boolean urgent = op.isUrgent();
            executor.execute(op, partitionId, urgent);
        } else {
            ExecutorService executor = executionService.getExecutor(executorName);
            if (executor == null) {
                throw new IllegalStateException("Could not found executor with name: " + executorName);
            }
            if (op instanceof PartitionAware) {
                throw new IllegalStateException("PartitionAwareOperation " + op + " can't be executed on a " +
                        "custom executor with name: " + executorName);
            }
            if (op instanceof UrgentSystemOperation) {
                throw new IllegalStateException("UrgentSystemOperation " + op + " can't be executed on a custom " +
                        "executor with name: " + executorName);
            }
            executor.execute(new LocalOperationProcessor(op));
        }
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

    private void processPacket(Packet packet) {
        final Connection conn = packet.getConn();
        try {
            final Address caller = conn.getEndPoint();
            final Data data = packet.getData();
            final Object object = nodeEngine.toObject(data);
            final Operation op = (Operation) object;
            op.setNodeEngine(nodeEngine);
            OperationAccessor.setCallerAddress(op, caller);
            OperationAccessor.setConnection(op, conn);

            ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, op);
            if (!isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                final Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                        op.getClass().getName(), op.getServiceName());
                handleOperationError(op, error);
            } else {
                String executorName = op.getExecutorName();
                if (executorName == null) {
                    processOperation(op);
                } else {
                    ExecutorService executor = executionService.getExecutor(executorName);
                    if (executor == null) {
                        throw new IllegalStateException("Could not found executor with name: " + executorName);
                    }
                    executor.execute(new LocalOperationProcessor(op));
                }
            }
        } catch (Throwable e) {
            logger.severe(e);
        }
    }

    /**
     * Runs operation in calling thread.
     */
    private void processOperation(final Operation op) {
        executedOperationsCount.incrementAndGet();

        RemoteCallKey callKey = null;
        try {
            if (isCallTimedOut(op)) {
                Object response = new CallTimeoutException(op.getClass().getName(), op.getInvocationTime(), op.getCallTimeout());
                op.getResponseHandler().sendResponse(response);
                return;
            }
            callKey = beforeCallExecution(op);
            final int partitionId = op.getPartitionId();
            if (op instanceof PartitionAwareOperation) {
                if (partitionId < 0) {
                    throw new IllegalArgumentException("Partition id cannot be negative! -> " + partitionId);
                }
                final InternalPartition internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
                if (retryDuringMigration(op) && internalPartition.isMigrating()) {
                    throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
                final Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
                if (op.validatesTarget() && !node.getThisAddress().equals(owner)) {
                    throw new WrongTargetException(node.getThisAddress(), owner, partitionId, op.getReplicaIndex(),
                            op.getClass().getName(), op.getServiceName());
                }
            }

            OperationAccessor.setStartTime(op, Clock.currentTimeMillis());
            op.beforeRun();
            if (op instanceof WaitSupport) {
                WaitSupport waitSupport = (WaitSupport) op;
                if (waitSupport.shouldWait()) {
                    nodeEngine.waitNotifyService.await(waitSupport);
                    return;
                }
            }
            op.run();

            final boolean returnsResponse = op.returnsResponse();
            Object response = null;
            if (op instanceof BackupAwareOperation) {
                final BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
                int syncBackupCount = 0;
                if (backupAwareOp.shouldBackup()) {
                    syncBackupCount = sendBackups(backupAwareOp);
                }
                if (returnsResponse) {
                    response = new NormalResponse(op.getResponse(), op.getCallId(), syncBackupCount, op.isUrgent());
                }
            }
            if (returnsResponse) {
                if (response == null) {
                    response = op.getResponse();
                }
                final ResponseHandler responseHandler = op.getResponseHandler();
                if (responseHandler == null) {
                    throw new IllegalStateException("ResponseHandler should not be null!");
                }
                responseHandler.sendResponse(response);
            }

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
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            afterCallExecution(op, callKey);
        }
    }


    private static boolean retryDuringMigration(Operation op) {
        return !(op instanceof ReadonlyOperation || OperationAccessor.isMigrationOperation(op));
    }

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

    private RemoteCallKey beforeCallExecution(Operation op) {
        RemoteCallKey callKey = null;
        if (op.getCallId() != 0 && op.returnsResponse()) {
            callKey = new RemoteCallKey(op);
            RemoteCallKey current;
            if ((current = executingCalls.put(callKey, callKey)) != null) {
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

    private int sendBackups(BackupAwareOperation backupAwareOp) throws Exception {
        Operation op = (Operation) backupAwareOp;
        boolean returnsResponse = op.returnsResponse();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();

        int maxBackupCount = InternalPartition.MAX_BACKUP_COUNT;
        int maxPossibleBackupCount = Math.min(partitionService.getMemberGroupsSize() - 1, maxBackupCount);

        int requestedSyncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? Math.min(maxBackupCount, backupAwareOp.getSyncBackupCount()) : 0;

        int requestedAsyncBackupCount = backupAwareOp.getAsyncBackupCount() > 0
                ? Math.min(maxBackupCount - requestedSyncBackupCount, backupAwareOp.getAsyncBackupCount()) : 0;

        int totalRequestedBackupCount = requestedSyncBackupCount + requestedAsyncBackupCount;
        if (totalRequestedBackupCount == 0) {
            return 0;
        }

        int partitionId = op.getPartitionId();
        long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(partitionId, totalRequestedBackupCount);

        int syncBackupCount = Math.min(maxPossibleBackupCount, requestedSyncBackupCount);
        int asyncBackupCount = Math.min(maxPossibleBackupCount - syncBackupCount, requestedAsyncBackupCount);
        if (!returnsResponse) {
            asyncBackupCount += syncBackupCount;
            syncBackupCount = 0;
        }

        int totalBackupCount = syncBackupCount + asyncBackupCount;
        if (totalBackupCount == 0) {
            return 0;
        }

        int sentSyncBackupCount = 0;
        String serviceName = op.getServiceName();
        InternalPartition partition = partitionService.getPartition(partitionId);

        for (int replicaIndex = 1; replicaIndex <= totalBackupCount; replicaIndex++) {
            Address target = partition.getReplicaAddress(replicaIndex);
            if (target != null) {
                if (target.equals(node.getThisAddress())) {
                    throw new IllegalStateException("Normally shouldn't happen! Owner node and backup node " +
                            "are the same! " + partition);
                } else {
                    Operation backupOp = backupAwareOp.getBackupOperation();
                    if (backupOp == null) {
                        throw new IllegalArgumentException("Backup operation should not be null!");
                    }

                    backupOp.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(serviceName);
                    Data backupOpData = nodeEngine.getSerializationService().toData(backupOp);

                    boolean isSyncBackup = replicaIndex <= syncBackupCount;
                    Backup backup = new Backup(backupOpData, op.getCallerAddress(), replicaVersions, isSyncBackup);
                    backup.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(serviceName)
                            .setCallerUuid(nodeEngine.getLocalMember().getUuid());
                    OperationAccessor.setCallId(backup, op.getCallId());
                    send(backup, target);

                    if (isSyncBackup) {
                        sentSyncBackupCount++;
                    }
                }
            }
        }
        return sentSyncBackupCount;
    }

    private void handleOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        op.logError(e);
        ResponseHandler responseHandler = op.getResponseHandler();
        if (op.returnsResponse() && responseHandler != null) {
            try {
                if (node.isActive()) {
                    responseHandler.sendResponse(e);
                } else if (responseHandler.isLocal()) {
                    responseHandler.sendResponse(new HazelcastInstanceNotActiveException());
                }
            } catch (Throwable t) {
                logger.warning("While sending op error... op: " + op + ", error: " + e, t);
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
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        Map<Address, List<Integer>> memberPartitions = nodeEngine.getPartitionService().getMemberPartitionsMap();
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                   Collection<Integer> partitions) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(3);
        for (int partition : partitions) {
            Address owner = waitUntilPartitionOwnerSet(partition);
            if (!memberPartitions.containsKey(owner)) {
                memberPartitions.put(owner, new ArrayList<Integer>());
            }
            memberPartitions.get(owner).add(partition);
        }
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    private Address waitUntilPartitionOwnerSet(int partition) throws InterruptedException {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Address owner = partitionService.getPartitionOwner(partition);
        while (owner == null) {
            Thread.sleep(100);
            owner = partitionService.getPartitionOwner(partition);
        }
        return owner;
    }

    private Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                    Map<Address, List<Integer>> memberPartitions) throws Exception {
        final Thread currentThread = Thread.currentThread();
        if (currentThread instanceof BasicOperationScheduler.PartitionThread) {
            throw new IllegalThreadStateException(currentThread + " cannot make invocation on multiple partitions!");
        }
        final Map<Address, Future> responses = new HashMap<Address, Future>(memberPartitions.size());
        for (Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            final Address address = mp.getKey();
            final List<Integer> partitions = mp.getValue();
            final PartitionIteratingOperation pi = new PartitionIteratingOperation(partitions, operationFactory);
            Future future = createInvocationBuilder(serviceName, pi, address).setTryCount(10).setTryPauseMillis(300).invoke();
            responses.put(address, future);
        }
        Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(
                nodeEngine.getPartitionService().getPartitionCount());
        for (Map.Entry<Address, Future> response : responses.entrySet()) {
            try {
                PartitionResponse result = (PartitionResponse) nodeEngine.toObject(response.getValue().get());
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
        final List<Integer> failedPartitions = new LinkedList<Integer>();
        for (Map.Entry<Integer, Object> partitionResult : partitionResults.entrySet()) {
            int partitionId = partitionResult.getKey();
            Object result = partitionResult.getValue();
            if (result instanceof Throwable) {
                failedPartitions.add(partitionId);
            }
        }
        for (Integer failedPartition : failedPartitions) {
            Future f = createInvocationBuilder(serviceName,
                    operationFactory.createOperation(), failedPartition).invoke();
            partitionResults.put(failedPartition, f);
        }
        for (Integer failedPartition : failedPartitions) {
            Future f = (Future) partitionResults.get(failedPartition);
            Object result = f.get();
            partitionResults.put(failedPartition, result);
        }
        return partitionResults;
    }

    @Override
    public boolean send(final Operation op, final Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        } else {
            return send(op, node.getConnectionManager().getOrConnect(target));
        }
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
        Packet packet = new Packet(data, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (response.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        return nodeEngine.send(packet, node.getConnectionManager().getOrConnect(target));
    }

    private boolean send(final Operation op, final Connection connection) {
        Data data = nodeEngine.toData(op);
        final int partitionId = getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP);
        if (op instanceof UrgentSystemOperation) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        return nodeEngine.send(packet, connection);
    }

    @PrivateApi
    long registerRemoteCall(RemoteCall call) {
        final long callId = newCallId();
        remoteCalls.put(callId, call);
        return callId;
    }

    @PrivateApi
    long newCallId() {
        final long callId = callIdGen.incrementAndGet();
        if (callId == 0) {
            return newCallId();
        }
        return callId;
    }

    @PrivateApi
    RemoteCall deregisterRemoteCall(long callId) {
        return remoteCalls.remove(callId);
    }

    @PrivateApi
    void registerBackupCall(long callId, BackupCompletionCallback backupCompletionCallback) {
        final BackupCompletionCallback current = backupCalls.put(callId, backupCompletionCallback);
        if (current != null) {
            logger.warning("Already registered a backup record for call[" + callId + "]!");
        }
    }

    @PrivateApi
    void deregisterBackupCall(long callId) {
        backupCalls.remove(callId);
    }

    @PrivateApi
    long getDefaultCallTimeout() {
        return defaultCallTimeout;
    }

    @PrivateApi
    boolean isOperationExecuting(Address callerAddress, String callerUuid, long operationCallId) {
        return executingCalls.containsKey(new RemoteCallKey(callerAddress, callerUuid, operationCallId));
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
                final Iterator<RemoteCall> iter = remoteCalls.values().iterator();
                while (iter.hasNext()) {
                    final RemoteCall call = iter.next();
                    if (call.isCallTarget(member)) {
                        iter.remove();
                        call.offerResponse(new MemberLeftException(member));
                    }
                }
            }
        }, 1111, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        logger.finest("Stopping operation threads...");
        responseExecutor.shutdown();
        final Object response = new HazelcastInstanceNotActiveException();
        for (RemoteCall call : remoteCalls.values()) {
            call.offerResponse(response);
        }
        remoteCalls.clear();
        backupCalls.clear();
        executor.shutdown();
    }

    public class BasicOperationProcessorImpl implements BasicOperationProcessor {
        @Override
        public void process(Object o) {
            if (o == null) {
                throw new IllegalArgumentException();
            } else if (o instanceof Operation) {
                processOperation((Operation) o);
            } else if (o instanceof Packet) {
                processPacket((Packet) o);
            } else if (o instanceof Runnable) {
                ((Runnable) o).run();
            } else {
                throw new IllegalArgumentException("Unrecognized task:" + o);
            }
        }
    }

    /**
     * Process the operation that has been send locally to this OperationService.
     */
    private class LocalOperationProcessor implements Runnable {
        private final Operation op;

        private LocalOperationProcessor(Operation op) {
            this.op = op;
        }

        @Override
        public void run() {
            processOperation(op);
        }
    }

    @Override
    public void notifyBackupCall(long callId) {
        try {
            final BackupCompletionCallback callback = backupCalls.get(callId);
            if (callback != null) {
                callback.signalOneBackupComplete();
            }
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    private class ResponseProcessor implements Runnable {
        final Packet packet;

        public ResponseProcessor(Packet packet) {
            this.packet = packet;
        }

        // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
        private void notifyRemoteCall(NormalResponse response) {
            RemoteCall call = deregisterRemoteCall(response.getCallId());
            if (call == null) {
                throw new HazelcastException("No call for response:" + response);
            }

            call.offerResponse(response);
        }

        @Override
        public void run() {
            try {
                final Data data = packet.getData();
                final Response response = (Response) nodeEngine.toObject(data);

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
    }

    private static class RemoteCallKey {
        private final long time = Clock.currentTimeMillis();
        private final Address callerAddress; // human readable caller
        private final String callerUuid;
        private final long callId;

        private RemoteCallKey(Address callerAddress, String callerUuid, long callId) {
            if (callerUuid == null) {
                throw new IllegalArgumentException("Caller UUID is required!");
            }
            this.callerAddress = callerAddress;
            if (callerAddress == null) {
                throw new IllegalArgumentException("Caller address is required!");
            }
            this.callerUuid = callerUuid;
            this.callId = callId;
        }

        private RemoteCallKey(final Operation op) {
            callerUuid = op.getCallerUuid();
            if (callerUuid == null) {
                throw new IllegalArgumentException("Caller UUID is required! -> " + op);
            }
            callerAddress = op.getCallerAddress();
            if (callerAddress == null) {
                throw new IllegalArgumentException("Caller address is required! -> " + op);
            }
            callId = op.getCallId();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteCallKey callKey = (RemoteCallKey) o;
            if (callId != callKey.callId) return false;
            if (!callerUuid.equals(callKey.callerUuid)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = callerUuid.hashCode();
            result = 31 * result + (int) (callId ^ (callId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("RemoteCallKey");
            sb.append("{callerAddress=").append(callerAddress);
            sb.append(", callerUuid=").append(callerUuid);
            sb.append(", callId=").append(callId);
            sb.append(", time=").append(time);
            sb.append('}');
            return sb.toString();
        }
    }
}

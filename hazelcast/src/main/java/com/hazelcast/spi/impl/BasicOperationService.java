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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.OperationAccessor.isJoinOperation;
import static com.hazelcast.spi.OperationAccessor.setCallId;

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

    final ConcurrentMap<Long, BasicInvocation> invocations;
    final BasicOperationScheduler scheduler;
    private final AtomicLong executedOperationsCount = new AtomicLong();

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong callIdGen = new AtomicLong(1);

    private final Map<RemoteCallKey, RemoteCallKey> executingCalls;

    private final long defaultCallTimeout;
    private final ExecutionService executionService;

    BasicOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class);
        this.defaultCallTimeout = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        this.executionService = nodeEngine.getExecutionService();

        int coreSize = Runtime.getRuntime().availableProcessors();
        boolean reallyMultiCore = coreSize >= 8;
        int concurrencyLevel = reallyMultiCore ? coreSize * 4 : 16;
        this.executingCalls = new ConcurrentHashMap<RemoteCallKey, RemoteCallKey>(1000, 0.75f, concurrencyLevel);
        this.invocations = new ConcurrentHashMap<Long, BasicInvocation>(1000, 0.75f, concurrencyLevel);
        this.scheduler = new BasicOperationScheduler(node, executionService, new BasicOperationProcessorImpl());
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
        return scheduler.getResponseQueueSize();
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
    public void receive(final Packet packet) {
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
            processOperation(op);
        } else {
            throw new IllegalThreadStateException("Operation: " + op + " cannot be run in current thread! -> "
                    + Thread.currentThread());
        }
    }

    /**
     * Executes operation in operation executor pool.
     *
     * @param op
     */
    @Override
    public void executeOperation(final Operation op) {
        scheduler.execute(op);
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

    private void processResponsePacket(Packet packet) {
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

    // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
    private void notifyRemoteCall(NormalResponse response) {
        BasicInvocation invocation = invocations.get(response.getCallId());
        if (invocation == null) {
            throw new HazelcastException("No invocation for response:" + response);
        }

        invocation.notify(response);
    }

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

    private void processOperationPacket(Packet packet) {
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
                    throw new IllegalStateException("Normally shouldn't happen! Owner node and backup node "
                            + "are the same! " + partition);
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
            Address owner = nodeEngine.getPartitionService().getPartitionOwner(partition);
            if (!memberPartitions.containsKey(owner)) {
                memberPartitions.put(owner, new ArrayList<Integer>());
            }
            memberPartitions.get(owner).add(partition);
        }
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    private Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                    Map<Address, List<Integer>> memberPartitions) throws Exception {
        final Thread currentThread = Thread.currentThread();
        if (currentThread instanceof BasicOperationScheduler.OperationThread) {
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

        int x = 0;
        for (Map.Entry<Address, Future> response : responses.entrySet()) {
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
            x++;
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
        }

        return send(op, node.getConnectionManager().getOrConnect(target));
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
        return nodeEngine.send(packet, node.getConnectionManager().getOrConnect(target));
    }

    private boolean send(final Operation op, final Connection connection) {
        Data data = nodeEngine.toData(op);

        //enable this line to get some logging of sizes of operations.
        //System.out.println(op.getClass()+" "+data.bufferSize());
        final int partitionId = scheduler.getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getPortableContext());
        packet.setHeader(Packet.HEADER_OP);
        if (op instanceof UrgentSystemOperation) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        return nodeEngine.send(packet, connection);
    }

    public long registerInvocation(BasicInvocation invocation) {
        long callId = callIdGen.getAndIncrement();
        Operation op = invocation.op;
        if (op.getCallId() != 0) {
            invocations.remove(op.getCallId());
        }

        invocations.put(callId, invocation);
        setCallId(invocation.op, callId);
        return callId;
    }

    public void deregisterInvocation(long id) {
        invocations.remove(id);
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
                final Iterator<BasicInvocation> iter = invocations.values().iterator();
                while (iter.hasNext()) {
                    final BasicInvocation invocation = iter.next();
                    if (invocation.isCallTarget(member)) {
                        iter.remove();
                        invocation.notify(new MemberLeftException(member));
                    }
                }
            }
        }, 1111, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        logger.finest("Stopping operation threads...");
        final Object response = new HazelcastInstanceNotActiveException();
        for (BasicInvocation invocation : invocations.values()) {
            invocation.notify(response);
        }
        invocations.clear();
        scheduler.shutdown();
    }

    public class BasicOperationProcessorImpl implements BasicOperationProcessor {

        @Override
        public void process(Object o) {
            if (o == null) {
                throw new IllegalArgumentException();
            } else if (o instanceof Operation) {
                processOperation((Operation) o);
            } else if (o instanceof Packet) {
                Packet packet = (Packet) o;
                if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                    processResponsePacket(packet);
                } else {
                    processOperationPacket(packet);
                }
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
    private final class LocalOperationProcessor implements Runnable {
        private final Operation op;

        private LocalOperationProcessor(Operation op) {
            this.op = op;
        }

        @Override
        public void run() {
            processOperation(op);
        }
    }

    private static final class RemoteCallKey {
        private final long time = Clock.currentTimeMillis();
        // human readable caller
        private final Address callerAddress;
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
            if (!callerUuid.equals(callKey.callerUuid)) {
                return false;
            }
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

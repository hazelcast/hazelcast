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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.*;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */
final class OperationServiceImpl implements OperationService {

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong remoteCallIdGen = new AtomicLong(0);
    private final ConcurrentMap<Long, RemoteCall> remoteCalls;
    private final Lock[] ownerLocks;
    private final ReadWriteLock[] partitionLocks;
    private final FastExecutor executor;
    private final long defaultCallTimeout;
    private final Set<RemoteCallKey> executingCalls;

    OperationServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class.getName());
        defaultCallTimeout = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        final int coreSize = Runtime.getRuntime().availableProcessors();
        final boolean reallyMultiCore = coreSize >= 8;
        remoteCalls = new ConcurrentHashMap<Long, RemoteCall>(1000, 0.75f, (reallyMultiCore ? coreSize * 4 : 16));
        final String poolNamePrefix = node.getThreadPoolNamePrefix("operation");
        final ThreadFactory threadFactory = new PoolExecutorThreadFactory(node.threadGroup, poolNamePrefix, node.getConfig().getClassLoader());
        final String type = node.getGroupProperties().OPERATION_EXECUTOR_TYPE.getString();
        final int coreThreadSize = coreSize * 2;
        if ("blocking".equals(type)) {
            executor = new BlockingFastExecutor(coreThreadSize, poolNamePrefix, threadFactory);
        } else if ("spinning".equals(type)) {
            executor = new SpinningFastExecutor(coreThreadSize, poolNamePrefix, threadFactory);
        } else {
            executor = reallyMultiCore ? new SpinningFastExecutor(coreThreadSize, poolNamePrefix, threadFactory)
                : new BlockingFastExecutor(coreThreadSize, poolNamePrefix, threadFactory);
        }
        executor.setInterceptor(new BlockingFastExecutor.WorkerLifecycleInterceptor() {
            public void beforeWorkerStart() {
                logger.log(Level.INFO, "Creating a new operation thread -> Core: " + executor.getCoreThreadSize()
                    + ", Current: " + (executor.getActiveThreadCount() + 1) + ", Max: " + executor.getMaxThreadSize());
            }
            public void afterWorkerTerminate() {
                logger.log(Level.INFO, "Destroying an operation thread -> Core: " + executor.getCoreThreadSize()
                        + ", Current: " + executor.getActiveThreadCount() + ", Max: " + executor.getMaxThreadSize());
            }
        });

        ownerLocks = new Lock[100000];
        for (int i = 0; i < ownerLocks.length; i++) {
            ownerLocks[i] = new ReentrantLock();
        }
        int partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        partitionLocks = new ReadWriteLock[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
//            partitionLocks[i] = new SpinReadWriteLock(1, TimeUnit.MILLISECONDS);
            partitionLocks[i] = new ReentrantReadWriteLock();
        }
        executingCalls = Collections.newSetFromMap(new ConcurrentHashMap<RemoteCallKey, Boolean>(1000, 0.75f, (reallyMultiCore ? coreSize * 4 : 16)));
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("Partition id cannot be negative!");
        return new InvocationBuilder(nodeEngine, serviceName, op, partitionId);
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return new InvocationBuilder(nodeEngine, serviceName, op, target);
    }

    @PrivateApi
    void handleOperation(final Packet packet) {
        try {
            executor.execute(new RemoteOperationProcessor(packet));
        } catch (RejectedExecutionException e) {
            if (nodeEngine.isActive()) {
                throw e;
            }
        }
    }

    /**
     * Executes operation in operation executor pool.
     *
     * @param op
     */
    public void executeOperation(final Operation op) {
        executor.execute(new OperationExecutor(op));
    }

    /**
     * Runs operation in caller thread.
     *
     * @param op
     */
    public void runOperation(final Operation op) {
        final ThreadContext threadContext = ThreadContext.getOrCreate();
        Lock partitionLock = null;
        Lock keyLock = null;
        RemoteCallKey callKey = null;
        try {
            if (isCallTimedOut(op)) {
                Object response = new CallTimeoutException("Call timed out for " + op.getClass().getName()
                        + ", call-time: " + op.getInvocationTime() + ", timeout: " + op.getCallTimeout());
                op.getResponseHandler().sendResponse(response);
                return;
            }
            threadContext.setCurrentOperation(op);
            callKey = beforeCallExecution(op);
            final int partitionId = op.getPartitionId();
            if (op instanceof PartitionAwareOperation) {
                if (partitionId < 0) {
                    throw new IllegalArgumentException("Partition id cannot be negative! -> " + partitionId);
                }
                if (!OperationAccessor.isMigrationOperation(op) && node.partitionService.isPartitionMigrating(partitionId)) {
                    throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
                final ReadWriteLock migrationLock = partitionLocks[partitionId];
                if (op instanceof PartitionLevelOperation) {
                    partitionLock = migrationLock.writeLock();
                    partitionLock.lockInterruptibly();
                } else {
                    final Lock tmpPartitionLock = migrationLock.readLock();
                    if (!tmpPartitionLock.tryLock(250, TimeUnit.MILLISECONDS)) {
                        throw new PartitionMigratingException(node.getThisAddress(), partitionId, op.getClass().getName(), op.getServiceName());
                    }
                    partitionLock = tmpPartitionLock;
                    final PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(partitionId);
                    if (partitionInfo == null) {
                        throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                                op.getClass().getName(), op.getServiceName());
                    }
                    final Address owner = partitionInfo.getReplicaAddress(op.getReplicaIndex());
                    if (op.validatesTarget() && !node.getThisAddress().equals(owner)) {
                        throw new WrongTargetException(node.getThisAddress(), owner, partitionId, op.getReplicaIndex(),
                                op.getClass().getName(), op.getServiceName());
                    }
                    if (op instanceof KeyBasedOperation && !(op instanceof BackupOperation)) {
                        final int hash = ((KeyBasedOperation) op).getKeyHash();
                        final Lock[] locks = ownerLocks;
                        keyLock = locks[Math.abs(hash) % locks.length];
                        keyLock.lockInterruptibly();
                    }
                }
            }
            doRunOperation(op);
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            afterCallExecution(op, callKey);
            if (keyLock != null) {
                keyLock.unlock();
            }
            if (partitionLock != null) {
                partitionLock.unlock();
            }
            threadContext.setCurrentOperation(null);
        }
    }

    boolean isCallTimedOut(Operation op) {
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

    public void runOperationUnderExistingLock(Operation op) {
        final ThreadContext threadContext = ThreadContext.getOrCreate();
        final Operation parentOperation = threadContext.getCurrentOperation();
        threadContext.setCurrentOperation(op);
        final RemoteCallKey callKey = beforeCallExecution(op);
        try {
            doRunOperation(op);
        } finally {
            afterCallExecution(op, callKey);
            threadContext.setCurrentOperation(parentOperation);
        }
    }

    private RemoteCallKey beforeCallExecution(Operation op) {
        RemoteCallKey callKey = null;
        if (op.getCallId() != 0 && op.returnsResponse()) {
            callKey = new RemoteCallKey(op.getCallerAddress(), op.getCallId());
            if (!executingCalls.add(callKey)) {
                logger.log(Level.SEVERE, "Duplicate Call record! -> " + callKey + " == " + op.getClass().getName());
            }
        }
        return callKey;
    }

    private void afterCallExecution(Operation op, RemoteCallKey callKey) {
        if (callKey != null && op.getCallId() != 0 && op.returnsResponse()) {
            if (!executingCalls.remove(callKey)) {
                logger.log(Level.SEVERE, "No Call record has been found: -> " + callKey + " == " + op.getClass().getName());
            }
        }
    }

    private void doRunOperation(Operation op) {
        OperationAccessor.setStartTime(op, Clock.currentTimeMillis());
        try {
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
            Response response = null;
            if (op instanceof BackupAwareOperation) {
                final BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
                if (backupAwareOp.shouldBackup()) {
                    int syncBackupCount = sendBackups(backupAwareOp);
                    if (returnsResponse) {
                        response = new Response(op.getResponse(), syncBackupCount);
                    }
                }
            }

            if (returnsResponse) {
                if (response == null) {
                    response = new Response(op.getResponse());
                }
                ResponseHandler responseHandler = op.getResponseHandler();
                if (responseHandler == null) {
                    throw new IllegalStateException("ResponseHandler should not be null!");
                }
                responseHandler.sendResponse(response);
            }

            op.afterRun();
            if (op instanceof Notifier) {
                final Notifier notifier = (Notifier) op;
                if (notifier.shouldNotify()) {
                    nodeEngine.waitNotifyService.notify(notifier);
                }
            }
        } catch (Throwable e) {
            handleOperationError(op, e);
        }
    }

    private int sendBackups(BackupAwareOperation backupAwareOp) throws Exception {
        final Operation op = (Operation) backupAwareOp;

        final int maxBackups = node.getClusterService().getSize() - 1;
        final int syncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? Math.min(maxBackups, backupAwareOp.getSyncBackupCount()) : 0;
        final int asyncBackupCount = (backupAwareOp.getAsyncBackupCount() > 0 && maxBackups > syncBackupCount)
                ? Math.min(maxBackups - syncBackupCount, backupAwareOp.getAsyncBackupCount()) : 0;

        final int totalBackupCount = syncBackupCount + asyncBackupCount;
        if (totalBackupCount > 0) {
            final String serviceName = op.getServiceName();
            final int partitionId = op.getPartitionId();
            final long version = nodeEngine.backupService.incrementAndGetVersion(partitionId);
            final PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(partitionId);
            for (int replicaIndex = 1; replicaIndex <= totalBackupCount; replicaIndex++) {
                final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                if (target != null) {
                    final Operation backupOp = backupAwareOp.getBackupOperation();
                    if (backupOp == null) {
                        throw new IllegalArgumentException("Backup operation should not be null!");
                    }
                    if (target.equals(node.getThisAddress())) {
                        throw new IllegalStateException("Normally shouldn't happen!!");
                    } else {
                        backupOp.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(serviceName);
                        Backup backup = new Backup(backupOp, op.getCallerAddress(), replicaIndex <= syncBackupCount);
                        backup.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
                        backup.setVersion(version);
                        OperationAccessor.setCallId(backup, op.getCallId());
                        send(backup, target);
                    }
                }
            }
        }
        return syncBackupCount;
    }

    private void handleOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        if (e instanceof RetryableException) {
            final Level level = op.returnsResponse() ? Level.FINEST : Level.WARNING;
            logger.log(level, "While executing op: " + op + " -> " + e.getClass() + ": " + e.getMessage());
        } else {
            final Level level = nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            logger.log(level, "While executing op: " + op + " -> " + e.getMessage(), e);
        }
        if (node.isActive() && op.returnsResponse() && op.getResponseHandler() != null) {
            op.getResponseHandler().sendResponse(e);
        }
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation operation) throws Exception {
        final ParallelOperationFactory operationFactory = new ParallelOperationFactory(operation, nodeEngine);
        return invokeOnAllPartitions(serviceName, operationFactory);
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, MultiPartitionOperationFactory operationFactory)
            throws Exception {
        final Map<Address, List<Integer>> memberPartitions = nodeEngine.getPartitionService().getMemberPartitionsMap();
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    public Map<Integer, Object> invokeOnPartitions(String serviceName, Operation operation,
                                                   List<Integer> partitions) throws Exception {
        final ParallelOperationFactory operationFactory = new ParallelOperationFactory(operation, nodeEngine);
        return invokeOnPartitions(serviceName, operationFactory, partitions);
    }

    public Map<Integer, Object> invokeOnPartitions(String serviceName, MultiPartitionOperationFactory operationFactory,
                                                   List<Integer> partitions) throws Exception {
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

    public Map<Integer, Object> invokeOnTargetPartitions(String serviceName, Operation operation,
                                                         Address target) throws Exception {
        final ParallelOperationFactory operationFactory = new ParallelOperationFactory(operation, nodeEngine);
        return invokeOnTargetPartitions(serviceName, operationFactory, target);
    }

    public Map<Integer, Object> invokeOnTargetPartitions(String serviceName, MultiPartitionOperationFactory operationFactory,
                                                         Address target) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(1);
        memberPartitions.put(target, nodeEngine.getPartitionService().getMemberPartitions(target));
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    private Map<Integer, Object> invokeOnPartitions(String serviceName, MultiPartitionOperationFactory operationFactory,
                                                    Map<Address, List<Integer>> memberPartitions) throws Exception {
        final Map<Address, Future> responses = new HashMap<Address, Future>(memberPartitions.size());
        for (Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            final Address address = mp.getKey();
            final List<Integer> partitions = mp.getValue();
            final PartitionIteratingOperation pi = new PartitionIteratingOperation(partitions, operationFactory);
            Invocation inv = createInvocationBuilder(serviceName, pi,
                    address).setTryCount(5).setTryPauseMillis(300).build();
            Future future = inv.invoke();
            responses.put(address, future);
        }
        final Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(nodeEngine.getPartitionService().getPartitionCount());
        for (Map.Entry<Address, Future> response : responses.entrySet()) {
            try {
                PartitionResponse result = (PartitionResponse) nodeEngine.toObject(response.getValue().get());
                partitionResults.putAll(result.asMap());
            } catch (Throwable t) {
                if (logger.isLoggable(Level.FINEST)) {
                    logger.log(Level.WARNING, t.getMessage(), t);
                } else {
                    logger.log(Level.WARNING, t.getMessage());
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
            Invocation inv = createInvocationBuilder(serviceName,
                    operationFactory.createOperation(), failedPartition).build();
            Future f = inv.invoke();
            partitionResults.put(failedPartition, f);
        }
        for (Integer failedPartition : failedPartitions) {
            Future f = (Future) partitionResults.get(failedPartition);
            Object result = f.get();
            partitionResults.put(failedPartition, result);
        }
        return partitionResults;
    }

    public boolean send(final Operation op, final int partitionId, final int replicaIndex) {
        Address target = nodeEngine.getPartitionService().getPartitionInfo(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.log(Level.WARNING, "No target available for partition: " + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, target);
    }

    public boolean send(final Operation op, final Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            op.setNodeEngine(nodeEngine);
            executeOperation(op); // TODO: not sure what to do here...
            return true;
        } else {
            return send(op, node.getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final Connection connection) {
        Data opData = nodeEngine.toData(op);
        Packet packet = new Packet(opData, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP, true);
        if (OperationAccessor.isMigrationOperation(op)) {
            packet.setHeader(Packet.HEADER_MIGRATION, true);
        }
        return nodeEngine.send(packet, connection);
    }

    @PrivateApi
    long registerRemoteCall(RemoteCall call) {
        final long callId = newRemoteCallId();
        remoteCalls.put(callId, call);
        return callId;
    }

    @PrivateApi
    long newRemoteCallId() {
        final long callId = remoteCallIdGen.incrementAndGet();
        if (callId == 0) {
            return newRemoteCallId();
        }
        return callId;
    }

    private RemoteCall deregisterRemoteCall(long id) {
        return remoteCalls.remove(id);
    }

    // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
    @PrivateApi
    void notifyRemoteCall(long callId, Object response) {
        RemoteCall call = deregisterRemoteCall(callId);
        if (call != null) {
            call.offerResponse(response);
        } else {
            throw new HazelcastException("No call with id: " + callId + ", Response: " + response);
        }
    }

    @PrivateApi
    long getDefaultCallTimeout() {
        return defaultCallTimeout;
    }

    @PrivateApi
    boolean isOperationExecuting(Address caller, long operationCallId) {
        return executingCalls.contains(new RemoteCallKey(caller, operationCallId));
    }

    void onMemberLeft(final MemberImpl member) {
        for (RemoteCall call : remoteCalls.values()) {
            call.onMemberLeft(member);
        }
    }

    void shutdown() {
        logger.log(Level.FINEST, "Stopping operation threads...");
        executor.shutdown();
        final Object response = new HazelcastInstanceNotActiveException();
        for (RemoteCall call : remoteCalls.values()) {
            call.offerResponse(response);
        }
        remoteCalls.clear();
        for (int i = 0; i < ownerLocks.length; i++) {
            ownerLocks[i] = null;
        }
    }

    private class OperationExecutor implements Runnable {
        private final Operation op;

        private OperationExecutor(Operation op) {
            this.op = op;
        }

        public void run() {
            runOperation(op);
        }
    }

    private class RemoteOperationProcessor implements Runnable {
        final Packet packet;

        public RemoteOperationProcessor(Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Operation op = (Operation) nodeEngine.toObject(data);
                op.setNodeEngine(nodeEngine);
                OperationAccessor.setCallerAddress(op, caller);
                OperationAccessor.setConnection(op, conn);
                if (op instanceof Response) {
                    processResponse((Response) op);
                } else {
                    final ResponseHandler responseHandler = ResponseHandlerFactory.createRemoteResponseHandler(nodeEngine, op);
                    if (!OperationAccessor.isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                        final Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                                op.getClass().getName(), op.getServiceName());
                        if (op.returnsResponse()) {
                            responseHandler.sendResponse(error);
                        } else {
                            throw error;
                        }
                    } else {
                        op.setResponseHandler(responseHandler);
                        runOperation(op);
                    }
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        void processResponse(Response response) {
            try {
                response.beforeRun();
                response.run();
                response.afterRun();
            } catch (Throwable e) {
                logger.log(Level.SEVERE, "While processing response...", e);
            }
        }
    }

    private class RemoteCallKey {
        private final Address caller;
        private final long callId;

        private RemoteCallKey(Address caller, long callId) {
            this.caller = caller;
            this.callId = callId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteCallKey callKey = (RemoteCallKey) o;
            if (callId != callKey.callId) return false;
            if (!caller.equals(callKey.caller)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = caller.hashCode();
            result = 31 * result + (int) (callId ^ (callId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("RemoteCallKey");
            sb.append("{caller=").append(caller);
            sb.append(", callId=").append(callId);
            sb.append('}');
            return sb.toString();
        }
    }
}

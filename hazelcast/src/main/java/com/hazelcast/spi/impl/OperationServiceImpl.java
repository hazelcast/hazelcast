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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */
final class OperationServiceImpl implements OperationService {

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong localIdGen = new AtomicLong();
    private final ConcurrentMap<Long, Call> mapCalls = new ConcurrentHashMap<Long, Call>(1000);
    private final Lock[] ownerLocks;
    private final Lock[] backupLocks;
    private final SpinReadWriteLock[] partitionLocks;
    private final FastExecutor executor;
    private final long defaultCallTimeout;
    private final Set<CallKey> executingCalls = Collections.newSetFromMap(new ConcurrentHashMap<CallKey, Boolean>());

    OperationServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class.getName());
        defaultCallTimeout = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        final int coreSize = Runtime.getRuntime().availableProcessors();
        final String poolNamePrefix = node.getThreadPoolNamePrefix("operation");
        executor = new FastExecutor(coreSize, poolNamePrefix,
                new PoolExecutorThreadFactory(node.threadGroup, poolNamePrefix, node.getConfig().getClassLoader()));

        ownerLocks = new Lock[100000];
        for (int i = 0; i < ownerLocks.length; i++) {
            ownerLocks[i] = new ReentrantLock();
        }
        backupLocks = new Lock[10000];
        for (int i = 0; i < backupLocks.length; i++) {
            backupLocks[i] = new ReentrantLock();
        }
        int partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        partitionLocks = new SpinReadWriteLock[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionLocks[i] = new SpinReadWriteLock(1, TimeUnit.MILLISECONDS);
        }
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("Partition id must be bigger than zero!");
        return new InvocationBuilder(nodeEngine, serviceName, op, partitionId);
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return new InvocationBuilder(nodeEngine, serviceName, op, target);
    }

    @PrivateApi
    public void handleOperation(final Packet packet) {
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
     * @param op
     */
    public void executeOperation(final Operation op) {
        executor.execute(new OperationExecutor(op));
    }

    /**
     * Runs operation in caller thread.
     * @param op
     */
    public void runOperation(final Operation op) {
        final ThreadContext threadContext = ThreadContext.getOrCreate();
        SpinLock partitionLock = null;
        Lock keyLock = null;
        CallKey callKey = null;
        try {
            if (isCallTimedOut(op)) {
                Object response = new CallTimeoutException("Call timed out for "
                        + op.getClass().getName()
                        + ", call-time: " + op.getInvocationTime()
                        + ", timeout: " + op.getCallTimeout());
                op.getResponseHandler().sendResponse(response);
                return;
            }
            threadContext.setCurrentOperation(op);
            callKey = beforeCallExecution(op);
            final int partitionId = op.getPartitionId();
            if (op instanceof PartitionAwareOperation) {
                if (partitionId < 0) {
                    throw new IllegalArgumentException();
                }
                if (!isMigrationOperation(op) && node.partitionService.isPartitionMigrating(partitionId)) {
                    throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
                SpinReadWriteLock migrationLock = partitionLocks[partitionId];
                if (op instanceof PartitionLevelOperation) {
                    partitionLock = migrationLock.writeLock();
                    partitionLock.lock();
                } else {
                    partitionLock = migrationLock.readLock();
                    if (!partitionLock.tryLock(500, TimeUnit.MILLISECONDS)) {
                        partitionLock = null;
                        throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                                op.getClass().getName(), op.getServiceName());
                    }
                    PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(partitionId);
                    final Address owner = partitionInfo.getReplicaAddress(op.getReplicaIndex());
                    final boolean validatesTarget = op.validatesTarget();
                    if (validatesTarget && !node.getThisAddress().equals(owner)) {
                        throw new WrongTargetException(node.getThisAddress(), owner, partitionId,
                                op.getClass().getName(), op.getServiceName());
                    }
                    if (op instanceof KeyBasedOperation) {
                        final int hash = ((KeyBasedOperation) op).getKeyHash();
                        Lock[] lockGroup = ownerLocks;
                        if (op instanceof BackupOperation) {
                            lockGroup = backupLocks;
                        }
                        keyLock = lockGroup[Math.abs(hash) % lockGroup.length];
                        keyLock.lock();
                    }
                }
            } else if (op instanceof MultiPartitionAwareOperation) {
                final int[] partitionIds = ((MultiPartitionAwareOperation) op).getPartitionIds();
                partitionLock = new MultiPartitionLock(partitionIds);
                partitionLock.lock();
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
        if (op.returnsResponse()) {
            final long now = nodeEngine.getClusterTime();
            final long callTimeout = op.getCallTimeout();
            final long invocationTime = op.getInvocationTime();
            if (invocationTime + callTimeout < now) {
                return true;
            }
        }
        return false;
    }

    public void runOperationUnderExistingLock(Operation op) {
        final ThreadContext threadContext = ThreadContext.getOrCreate();
        final Operation parentOperation = threadContext.getCurrentOperation();
        threadContext.setCurrentOperation(op);
        final CallKey callKey = beforeCallExecution(op);
        try {
            doRunOperation(op);
        } finally {
            afterCallExecution(op, callKey);
            threadContext.setCurrentOperation(parentOperation);
        }
    }

    private CallKey beforeCallExecution(Operation op) {
        CallKey callKey = null;
        if (op.getCallId() > -1 && op.returnsResponse()) {
            callKey = new CallKey(op.getCallerAddress(), op.getCallId());
            if (!executingCalls.add(callKey)) {
                logger.log(Level.SEVERE, "Duplicate Call record! -> " + callKey + " == " + op.getClass().getName());
            }
        }
        return callKey;
    }

    private void afterCallExecution(Operation op, CallKey callKey) {
        if (callKey != null && op.getCallId() > -1 && op.returnsResponse()) {
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
                WaitSupport so = (WaitSupport) op;
                if (so.shouldWait()) {
                    nodeEngine.waitNotifyService.await(so);
                    return;
                }
            }
            op.run();
            if (op instanceof BackupAwareOperation) {
                final BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
                if (backupAwareOp.shouldBackup()) {
                    handleBackupAndSendResponse(backupAwareOp);
                } else {
                    sendResponse(op, null);
                }
            } else {
                sendResponse(op, null);
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

    private void handleBackupAndSendResponse(BackupAwareOperation backupAwareOp) throws Exception {
        final int maxRetryCount = 50;
        final int maxBackups = node.getClusterService().getSize() - 1;
        final int syncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? Math.min(maxBackups, backupAwareOp.getSyncBackupCount()) : 0;
        final int asyncBackupCount = (backupAwareOp.getAsyncBackupCount() > 0 && maxBackups > syncBackupCount)
                ? Math.min(maxBackups - syncBackupCount, backupAwareOp.getAsyncBackupCount()) : 0;
        Collection<BackupFuture> syncBackups = null;
        Collection<BackupFuture> asyncBackups = null;
        final Operation op = (Operation) backupAwareOp;
        final boolean returnsResponse = op.returnsResponse();
        final Operation backupOp;
        Operation backupResponse = null;
        if ((syncBackupCount + asyncBackupCount > 0) && (backupOp = backupAwareOp.getBackupOperation()) != null) {
            final String serviceName = op.getServiceName();
            final int partitionId = op.getPartitionId();
            final PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(partitionId);
            if (syncBackupCount > 0) {
                syncBackups = new ArrayList<BackupFuture>(syncBackupCount);
                for (int replicaIndex = 1; replicaIndex <= syncBackupCount; replicaIndex++) {
                    final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                    if (target != null) {
                        if (target.equals(node.getThisAddress())) {
                            throw new IllegalStateException("Normally shouldn't happen!!");
                        } else {
                            if (op.returnsResponse() && target.equals(op.getCallerAddress())) {
//                                TODO: fix me! what if backup migrates after response is returned?
                                backupOp.setServiceName(serviceName).setReplicaIndex(replicaIndex).setPartitionId(partitionId);
                                backupResponse = backupOp;
                            } else {
                                final Future f = createInvocationBuilder(serviceName, backupOp, partitionId)
                                        .setReplicaIndex(replicaIndex).setTryCount(maxRetryCount).build().invoke();
                                if (returnsResponse) {
                                    syncBackups.add(new BackupFuture(f, partitionId, replicaIndex, maxRetryCount));
                                }
                            }
                        }
                    }
                }
            }
            if (asyncBackupCount > 0) {
                asyncBackups = new ArrayList<BackupFuture>(asyncBackupCount);
                for (int replicaIndex = syncBackupCount + 1; replicaIndex <= asyncBackupCount; replicaIndex++) {
                    final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                    if (target != null) {
                        if (target.equals(node.getThisAddress())) {
                            throw new IllegalStateException("Normally shouldn't happen!!");
                        } else {
                            final Future f = createInvocationBuilder(serviceName, backupOp, partitionId)
                                    .setReplicaIndex(replicaIndex).setTryCount(maxRetryCount).build().invoke();
                            if (returnsResponse) {
                                asyncBackups.add(new BackupFuture(f, partitionId, replicaIndex, maxRetryCount));
                            }
                        }
                    }
                }
            }
        }
        final Object response = op.returnsResponse()
                ? (backupResponse == null ? op.getResponse() :
                new MultiResponse(nodeEngine.getSerializationService(), backupResponse, op.getResponse())) : null;
        waitBackupResponses(syncBackups);
        sendResponse(op, response);
        waitBackupResponses(asyncBackups);
    }

    private class BackupFuture {
        final Future future;
        final int partitionId;
        final int replicaIndex;
        final int retryCount;
        int retries;

        BackupFuture(Future future, int partitionId, int replicaIndex, int retryCount) {
            this.future = future;
            this.partitionId = partitionId;
            this.replicaIndex = replicaIndex;
            this.retryCount = retryCount;
        }

        Object get(int timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        boolean canRetry() {
            return retries++ < retryCount;
        }
    }

    private void waitBackupResponses(final Collection<BackupFuture> futures) throws ExecutionException {
        while (futures != null && !futures.isEmpty()) {
            final Iterator<BackupFuture> iter = futures.iterator();
            ExecutionException lastError = null;
            while (iter.hasNext()) {
                final BackupFuture f = iter.next();
                try {
                    if (f.canRetry()) {
                        f.get(500, TimeUnit.MILLISECONDS);
                        lastError = null;
                    }
                    iter.remove();
                    if (lastError != null) {
                        logger.log(Level.WARNING, "While backing up -> " + lastError.getMessage(), lastError);
                    }
                } catch (InterruptedException ignored) {
                } catch (TimeoutException ignored) {
                } catch (ExecutionException e) {
                    if (!ExceptionUtil.isRetryableException(e)) {
                        throw e;
                    } else if (nodeEngine.getClusterService().getSize() <= f.replicaIndex) {
                        iter.remove();
                    } else {
                        lastError = e;
                    }
                }
            }
        }
    }

    private void handleOperationError(Operation op, Throwable e) {
        if (e instanceof RetryableException) {
            final Level level = op.returnsResponse() ? Level.FINEST : Level.WARNING;
            logger.log(level, "While executing op: " + op + " -> " + e.getClass() + ": " + e.getMessage());
        } else {
            final Level level = nodeEngine.isActive() ? Level.SEVERE: Level.FINEST;
            logger.log(level, "While executing op: " + op + " -> " + e.getMessage(), e);
        }
        if (node.isActive()) {
            sendResponse(op, e);
        }
    }

    private void sendResponse(Operation op, Object response) {
        if (op.returnsResponse()) {
            ResponseHandler responseHandler = op.getResponseHandler();
            if (responseHandler == null) {
                throw new IllegalStateException("ResponseHandler should not be null!");
            }
            responseHandler.sendResponse(response == null ? op.getResponse() : response);
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
            if(!memberPartitions.containsKey(owner)){
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

    public void takeBackups(String serviceName, Operation op, int partitionId, int offset, int backupCount, int timeoutSeconds)
            throws ExecutionException, TimeoutException, InterruptedException {
        op.setServiceName(serviceName);
        backupCount = Math.min(node.getClusterService().getSize() - 1, backupCount);
        if (backupCount > 0) {
            List<Future> backupOps = new ArrayList<Future>(backupCount);
            PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(partitionId);
            for (int i = 0; i < backupCount; i++) {
                int replicaIndex = i + 1;
                Address replicaTarget = partitionInfo.getReplicaAddress(replicaIndex);
                if (replicaTarget != null) {
                    if (replicaTarget.equals(node.getThisAddress())) {
                        // Normally shouldn't happen!!
                        throw new IllegalStateException("Normally shouldn't happen!!");
                    } else {
                        backupOps.add(createInvocationBuilder(serviceName, op, partitionId).setReplicaIndex(replicaIndex)
                                .build().invoke());
                    }
                }
            }
            for (Future backupOp : backupOps) {
                backupOp.get(timeoutSeconds, TimeUnit.SECONDS);
            }
        }
    }

    public boolean send(final Operation op, final int partitionId, final int replicaIndex) {
        Address target = nodeEngine.getPartitionService().getPartitionInfo(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.log(Level.WARNING, "No target available for partition: "
                    + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, target);
    }

    public boolean send(final Operation op, final Address target) {
        if (target == null || nodeEngine.getThisAddress().equals(target)) {
            op.setNodeEngine(nodeEngine);
            runOperation(op); // TODO: not sure what to do here...
            return true;
        } else {
            return send(op, node.getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final Connection connection) {
        Data opData = nodeEngine.toData(op);
        Packet packet = new Packet(opData, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP, true);
        return nodeEngine.send(packet, connection);
    }

    @PrivateApi
    long registerCall(Call call) {
        long callId = localIdGen.incrementAndGet();
        mapCalls.put(callId, call);
        return callId;
    }

    private Call deregisterRemoteCall(long id) {
        return mapCalls.remove(id);
    }

    // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
    @PrivateApi
    void notifyCall(long callId, Object response) {
        Call call = deregisterRemoteCall(callId);
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
        return executingCalls.contains(new CallKey(caller, operationCallId));
    }

//    void onMemberDisconnect(Address disconnectedAddress) {
//        for (Call call : mapCalls.values()) {
//            call.onDisconnect(disconnectedAddress);
//        }
//    }

    void onMemberLeft(final MemberImpl member) {
        for (Call call : mapCalls.values()) {
            call.onMemberLeft(member);
        }
    }

    void shutdown() {
        logger.log(Level.FINEST, "Stopping operation threads...");
        executor.shutdown();
        mapCalls.clear();
        for (int i = 0; i < ownerLocks.length; i++) {
            ownerLocks[i] = null;
        }
        for (int i = 0; i < backupLocks.length; i++) {
            backupLocks[i] = null;
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

    private class MultiPartitionLock implements SpinLock {

        final int[] partitions;

        private MultiPartitionLock(int[] partitions) {
            this.partitions = partitions;
        }

        public void lock() {
            for (int partition : partitions) {
                partitionLocks[partition].readLock().lock();
            }
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        public void unlock() {
            for (int partition : partitions) {
                partitionLocks[partition].readLock().unlock();
            }
        }
    }

    private class RemoteOperationProcessor implements Runnable {
        private final Packet packet;

        public RemoteOperationProcessor(Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Operation op = (Operation) nodeEngine.toObject(data);
                op.setNodeEngine(nodeEngine).setCallerAddress(caller);
                op.setConnection(conn);
                if (op instanceof ResponseOperation) {
                    processResponse(op);
                } else {
                    ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, op);
                    runOperation(op);
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
//                send(new ErrorResponse(node.getThisAddress(), e), conn);
            }
        }

        private void processResponse(Operation op) {
            try {
                op.beforeRun();
                op.run();
                op.afterRun();
            } catch (Throwable e) {
                logger.log(Level.SEVERE, "While processing response...", e);
            }
        }
    }

    private class CallKey {
        private final Address caller;
        private final long callId;

        private CallKey(Address caller, long callId) {
            this.caller = caller;
            this.callId = callId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CallKey callKey = (CallKey) o;
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
            sb.append("CallKey");
            sb.append("{caller=").append(caller);
            sb.append(", callId=").append(callId);
            sb.append('}');
            return sb.toString();
        }
    }

    private static final ClassLoader thisClassLoader = OperationService.class.getClassLoader();

    private static boolean isMigrationOperation(Operation op) {
        return op instanceof MigrationCycleOperation
                && op.getClass().getClassLoader() == thisClassLoader;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("nodeEngineImpl");
        sb.append("{node=").append(node);
        sb.append('}');
        return sb.toString();
    }
}

/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.JoinOperation;
import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.executor.ExecutorThreadFactory;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.WaitNotifyService.WaitingOp;
import com.hazelcast.spi.impl.WaitNotifyService.WaitingOpProcessor;
import com.hazelcast.transaction.TransactionImpl;
import com.hazelcast.util.SpinLock;
import com.hazelcast.util.SpinReadWriteLock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */
final class InvocationServiceImpl implements InvocationService {

    private final NodeEngineImpl nodeService;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong localIdGen = new AtomicLong();
    private final ConcurrentMap<Long, Call> mapCalls = new ConcurrentHashMap<Long, Call>(1000);
    private final Lock[] ownerLocks = new Lock[100000];
    private final Lock[] backupLocks = new Lock[1000];
    private final SpinReadWriteLock[] partitionLocks;
    private final WaitNotifySupport waitNotifySupport;

    InvocationServiceImpl(NodeEngineImpl nodeService) {
        this.nodeService = nodeService;
        this.node = nodeService.getNode();
        this.logger = node.getLogger(InvocationService.class.getName());
        for (int i = 0; i < ownerLocks.length; i++) {
            ownerLocks[i] = new ReentrantLock();
        }
        for (int i = 0; i < backupLocks.length; i++) {
            backupLocks[i] = new ReentrantLock();
        }
        int partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        partitionLocks = new SpinReadWriteLock[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionLocks[i] = new SpinReadWriteLock(1, TimeUnit.MILLISECONDS);
        }
        waitNotifySupport = new WaitNotifySupport(new WaitingOpProcessorImpl());
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("Partition id must be bigger than zero!");
        return new InvocationBuilder(nodeService, serviceName, op, partitionId);
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return new InvocationBuilder(nodeService, serviceName, op, target);
    }

    void invoke(final InvocationImpl inv) {
        final Operation op = inv.getOperation();
        checkOperation(op);
        final Address target = inv.getTarget();
        final int partitionId = inv.getPartitionId();
        final int replicaIndex = inv.getReplicaIndex();
        final String serviceName = inv.getServiceName();
        final Address thisAddress = node.getThisAddress();
        op.setNodeEngine(nodeService).setServiceName(serviceName).setCaller(thisAddress)
                .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        if (target == null) {
            throw new WrongTargetException(thisAddress, target, partitionId,
                    op.getClass().getName(), serviceName);
        }
        if (!isJoinOperation(op) && nodeService.getClusterService().getMember(target) == null) {
            throw new TargetNotMemberException(target, partitionId, op.getClass().getName(), serviceName);
        }
        if (thisAddress.equals(target)) {
            ResponseHandlerFactory.setLocalResponseHandler(inv);
            runOperation(op);
        } else {
            Call call = new Call(target, inv);
            long callId = registerCall(call);
            op.setCallId(callId);
            boolean sent = send(op, target);
            if (!sent) {
                inv.setResult(new RetryableException(new IOException("Packet not sent!")));
            }
        }
    }

    private void checkOperation(Operation op) {
        final Operation parentOp = (Operation) ThreadContext.get().getCurrentOperation();
        boolean allowed = true;
        if (parentOp != null) {
            if (op instanceof BackupOperation) {
                // OK!
            } else if (parentOp instanceof PartitionLevelOperation) {
                if (op instanceof PartitionLevelOperation
                        && op.getPartitionId() == parentOp.getPartitionId()) {
                    // OK!
                } else if (!(op instanceof PartitionAwareOperation)) {
                    // OK!
                } else {
                    allowed = false;
                }
            } else if (parentOp instanceof KeyBasedOperation) {
                if (op instanceof PartitionLevelOperation) {
                    allowed = false;
                } else if (op instanceof KeyBasedOperation
                        && ((KeyBasedOperation) parentOp).getKeyHash() == ((KeyBasedOperation) op).getKeyHash()
                        && parentOp.getPartitionId() == op.getPartitionId()) {
                    // OK!
                } else if (op instanceof PartitionAwareOperation
                        && op.getPartitionId() == parentOp.getPartitionId()) {
                    // OK!
                } else if (!(op instanceof PartitionAwareOperation)) {
                    // OK!
                } else {
                    allowed = false;
                }
            } else if (parentOp instanceof PartitionAwareOperation) {
                if (op instanceof PartitionLevelOperation) {
                    allowed = false;
                } else if (op instanceof PartitionAwareOperation
                        && op.getPartitionId() == parentOp.getPartitionId()) {
                    // OK!
                } else if (!(op instanceof PartitionAwareOperation)) {
                    // OK!
                } else {
                    allowed = false;
                }
            }
        }
        if (!allowed) {
            throw new HazelcastException("INVOCATION IS NOT ALLOWED! ParentOp: "
                    + parentOp + ", CurrentOp: " + op);
        }
    }

    @PrivateApi
    public void handleOperation(final Packet packet) {
        final Executor executor = packet.isHeaderSet(Packet.HEADER_EVENT)
                ? nodeService.executionService.eventExecutorService : nodeService.executionService.cachedExecutorService;
        executor.execute(new RemoteOperationExecutor(packet));
    }

    public void runOperation(final Operation op) {
        final ThreadContext threadContext = ThreadContext.get();
        threadContext.setCurrentOperation(op);
        SpinLock partitionLock = null;
        Lock keyLock = null;
        try {
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
                    PartitionInfo partitionInfo = nodeService.getPartitionInfo(partitionId);
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
            }
            runOperationUnderExistingLock(op);
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            if (keyLock != null) {
                keyLock.unlock();
            }
            if (partitionLock != null) {
                partitionLock.unlock();
            }
            threadContext.setCurrentOperation(null);
        }
    }

    void runOperationUnderExistingLock(Operation op) {
        final ThreadContext threadContext = ThreadContext.get();
        final Object parentOperation = threadContext.getCurrentOperation();
        threadContext.setCurrentOperation(op);
        try {
            op.beforeRun();
            if (op instanceof WaitSupport) {
                WaitSupport so = (WaitSupport) op;
                if (so.shouldWait()) {
                    waitNotifySupport.wait(so);
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
                    waitNotifySupport.notify(notifier);
                }
            }
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            threadContext.setCurrentOperation(parentOperation);
        }
    }

    private void handleBackupAndSendResponse(BackupAwareOperation backupAwareOp) throws Exception {
        Object response = null;
        final int maxBackups = node.getClusterService().getSize() - 1;

        final int syncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? Math.min(maxBackups, backupAwareOp.getSyncBackupCount()) : 0;

        final int asyncBackupCount = (backupAwareOp.getAsyncBackupCount() > 0 && maxBackups > syncBackupCount)
                ? Math.min(maxBackups - syncBackupCount, backupAwareOp.getAsyncBackupCount()) : 0;

        Collection<Future> syncBackups = null;
        Collection<Future> asyncBackups = null;

        final Operation op = (Operation) backupAwareOp;
        final boolean returnsResponse = op.returnsResponse();
        final Operation backupOp;
        Operation backupResponse = null;
        if ((syncBackupCount + asyncBackupCount > 0) && (backupOp = backupAwareOp.getBackupOperation()) != null) {
            final String serviceName = op.getServiceName();
            final int partitionId = op.getPartitionId();
            final PartitionInfo partitionInfo = nodeService.getPartitionInfo(partitionId);

            if (syncBackupCount > 0) {
                syncBackups = new ArrayList<Future>(syncBackupCount);
                for (int replicaIndex = 1; replicaIndex <= syncBackupCount; replicaIndex++) {
                    final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                    if (target != null) {
                        if (target.equals(node.getThisAddress())) {
                            throw new IllegalStateException("Normally shouldn't happen!!");
                        } else {
                            if (op.returnsResponse() && target.equals(op.getCaller())) {
                                backupOp.setServiceName(serviceName).setReplicaIndex(replicaIndex).setPartitionId(partitionId);
                                backupResponse = backupOp;    // TODO: fix me! what if backup migrates after response is returned?
                            } else {
                                final Future f = createInvocationBuilder(serviceName, backupOp, partitionId)
                                        .setReplicaIndex(replicaIndex).build().invoke();
                                if (returnsResponse) {
                                    syncBackups.add(f);
                                }
                            }
                        }
                    }
                }
            }
            if (asyncBackupCount > 0) {
                asyncBackups = new ArrayList<Future>(asyncBackupCount);
                for (int replicaIndex = syncBackupCount + 1; replicaIndex <= asyncBackupCount; replicaIndex++) {
                    final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                    if (target != null) {
                        if (target.equals(node.getThisAddress())) {
                            throw new IllegalStateException("Normally shouldn't happen!!");
                        } else {
                            final Future f = createInvocationBuilder(serviceName, backupOp, partitionId)
                                    .setReplicaIndex(replicaIndex).build().invoke();
                            if (returnsResponse) {
                                asyncBackups.add(f);
                            }
                        }
                    }
                }
            }
        }

        response = op.returnsResponse()
                ? (backupResponse == null ? op.getResponse() : new MultiResponse(backupResponse, op.getResponse()))
                : null;

        waitFutureResponses(syncBackups);
        sendResponse(op, response);
        waitFutureResponses(asyncBackups);
    }

    private void waitFutureResponses(final Collection<Future> futures) throws ExecutionException {
        int size = futures != null ? futures.size() : 0;
        while (size > 0) {
            for (Future f : futures) {
                if (!f.isDone()) {
                    try {
                        f.get(1, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    } catch (TimeoutException ignored) {
                    }
                    if (f.isDone()) {
                        size--;
                    }
                }
            }
        }
    }

    private void handleOperationError(Operation op, Throwable e) {
        if (e instanceof RetryableException) {
            logger.log(Level.WARNING, "While executing op: " + op + " -> "
                    + e.getClass() + ": " + e.getMessage());
            logger.log(Level.FINEST, e.getMessage(), e);
        } else {
            logger.log(Level.SEVERE, "While executing op: " + op + " -> "
                    + e.getMessage(), e);
        }
        sendResponse(op, e);
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

    public void takeBackups(String serviceName, Operation op, int partitionId, int offset, int backupCount, int timeoutSeconds)
            throws ExecutionException, TimeoutException, InterruptedException {
        op.setServiceName(serviceName);
        backupCount = Math.min(node.getClusterService().getSize() - 1, backupCount);
        if (backupCount > 0) {
            List<Future> backupOps = new ArrayList<Future>(backupCount);
            PartitionInfo partitionInfo = nodeService.getPartitionInfo(partitionId);
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
        Address target = nodeService.getPartitionInfo(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.log(Level.WARNING, "No target available for partition: "
                    + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, target);
    }

    public boolean send(final Operation op, final Address target) {
        if (target == null || nodeService.getThisAddress().equals(target)) {
            op.setNodeEngine(nodeService);
            runOperation(op); // TODO: not sure what to do here...
            return true;
        } else {
            return send(op, nodeService.getNode().getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final Connection connection) {
        Data opData = IOUtil.toData(op);
        final Packet packet = new Packet(opData, connection);
        packet.setHeader(Packet.HEADER_EVENT, op instanceof EventOperation);
        return node.clusterService.send(packet, connection);
    }

    private long registerCall(Call call) {
        long callId = localIdGen.incrementAndGet();
        mapCalls.put(callId, call);
        return callId;
    }

    private Call deregisterRemoteCall(long id) {
        return mapCalls.remove(id);
    }

    @PrivateApi
    void notifyCall(long callId, Object response) {
        Call call = deregisterRemoteCall(callId);
        if (call != null) {
            call.offerResponse(response);
        } else {
            throw new HazelcastException("No call with id: " + callId + ", Response: " + response);
        }
    }

    private class RemoteOperationExecutor implements Runnable {
        private final Packet packet;

        private RemoteOperationExecutor(final Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final Data data = packet.getValue();
            final Address caller = packet.getConn().getEndPoint();
            try {
                final Operation op = (Operation) IOUtil.toObject(data);
                op.setNodeEngine(nodeService).setCaller(caller);
                op.setConnection(packet.getConn());
                if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
                    op.setResponseHandler(ResponseHandlerFactory.NO_RESPONSE_HANDLER);
                    nodeService.eventService.onEvent((EventOperation) op);
                } else {
                    ResponseHandlerFactory.setRemoteResponseHandler(nodeService, op);
                    runOperation(op);
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                send(new ErrorResponse(node.getThisAddress(), e), packet.getConn());
            }
        }
    }

    void onMemberDisconnect(Address deadAddress) {
        for (Call call : mapCalls.values()) {
            call.onDisconnect(deadAddress);
        }
    }

    void shutdown() {
        mapCalls.clear();
    }

    private class WaitingOpProcessorImpl implements WaitNotifySupport.WaitingOpProcessor {

        public void process(final WaitNotifySupport.WaitingOp so) throws Exception {
            nodeService.executionService.execute(new Runnable() {
                public void run() {
                    runOperation(so);
                }
            });
        }

        public void processUnderExistingLock(Operation operation) {
            runOperationUnderExistingLock(operation);
        }
    }

    private static final ClassLoader thisClassLoader = InvocationService.class.getClassLoader();

    private static boolean isMigrationOperation(Operation op) {
        return op instanceof MigrationCycleOperation
                && op.getClass().getClassLoader() == thisClassLoader
                && op.getClass().getName().startsWith("com.hazelcast.partition");
    }

    private static boolean isJoinOperation(Operation op) {
        return op instanceof JoinOperation
                && op.getClass().getClassLoader() == thisClassLoader
                && op.getClass().getName().startsWith("com.hazelcast.cluster");
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NodeServiceImpl");
        sb.append("{node=").append(node);
        sb.append('}');
        return sb.toString();
    }
}

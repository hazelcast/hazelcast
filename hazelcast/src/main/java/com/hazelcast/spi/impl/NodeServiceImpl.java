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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class NodeServiceImpl implements NodeService {

    private final Node node;
    private final ILogger logger;
    private final ExecutorService cachedExecutorService;
    private final ExecutorService eventExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final int partitionCount;
    private final Lock[] ownerLocks = new Lock[100000];
    private final Lock[] backupLocks = new Lock[1000];
    private final ConcurrentMap<Long, Call> mapCalls = new ConcurrentHashMap<Long, Call>(1000);
    private final AtomicLong localIdGen = new AtomicLong();
    private final ServiceManager serviceManager;
    private final WaitNotifyService waitNotifyService;
    private final EventService eventService;

    public NodeServiceImpl(Node node) {
        this.node = node;
        logger = node.getLogger(NodeService.class.getName());
        for (int i = 0; i < ownerLocks.length; i++) {
            ownerLocks[i] = new ReentrantLock();
        }
        for (int i = 0; i < backupLocks.length; i++) {
            backupLocks[i] = new ReentrantLock();
        }
        partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        final ClassLoader classLoader = node.getConfig().getClassLoader();
        final ExecutorThreadFactory threadFactory = new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                node.getThreadPoolNamePrefix("cached"), classLoader);
        cachedExecutorService = new ThreadPoolExecutor(
                3, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory);
//        cachedExecutorService = Executors.newFixedThreadPool(40, threadFactory);
        eventExecutorService = Executors.newSingleThreadExecutor(
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("event"), node.getConfig().getClassLoader()));
        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                new ExecutorThreadFactory(node.threadGroup,
                        node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("scheduled"), classLoader));
        serviceManager = new ServiceManager(this);
        waitNotifyService = new WaitNotifyService(new WaitingOpProcessorImpl());
        eventService = new EventService(this);
    }

    public void start() {
        serviceManager.startServices();
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("Partition id must be bigger than zero!");
        return new InvocationBuilder(this, serviceName, op, partitionId);
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return new InvocationBuilder(this, serviceName, op, target);
    }

    void invoke(final InvocationImpl inv) {
        final Operation op = inv.getOperation();
        checkOperation(op);
        final Address target = inv.getTarget();
        final int partitionId = inv.getPartitionId();
        final int replicaIndex = inv.getReplicaIndex();
        final String serviceName = inv.getServiceName();
        op.setNodeService(this).setServiceName(serviceName).setCaller(getThisAddress())
                .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        if (target == null) {
            throw new WrongTargetException(getThisAddress(), target, partitionId,
                    op.getClass().getName(), serviceName);
        }
        if (!isJoinOperation(op) && getClusterService().getMember(target) == null) {
            throw new TargetNotMemberException(target, partitionId, op.getClass().getName(), serviceName);
        }
        if (getThisAddress().equals(target)) {
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
        final Executor executor = packet.isHeaderSet(Packet.HEADER_EVENT) ? eventExecutorService : cachedExecutorService;
        executor.execute(new RemoteOperationExecutor(packet));
    }

    public boolean send(final Operation op, final int partitionId, final int replicaIndex) {
        Address target = getPartitionInfo(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.log(Level.WARNING, "No target available for partition: "
                    + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, target);
    }

    public boolean send(final Operation op, final Address target) {
        if (target == null || getThisAddress().equals(target)) {
            op.setNodeService(this);
            runOperation(op); // TODO: not sure what to do here...
            return true;
        } else {
            return send(op, node.getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final Connection connection) {
        Data opData = toData(op);
        final Packet packet = new Packet(opData, connection);
        packet.setHeader(Packet.HEADER_EVENT, op instanceof EventOperation);
        return node.clusterService.send(packet, connection);
    }

    @PrivateApi
    public void onMemberLeft(MemberImpl member) {

        onMemberDisconnect(member.getAddress());
    }

    @PrivateApi
    public void onMemberDisconnect(Address deadAddress) {
        for (Call call : mapCalls.values()) {
            call.onDisconnect(deadAddress);
        }
    }

    @PrivateApi
    public void onPartitionMigrate(MigrationInfo migrationInfo) {

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

    @PrivateApi
    public <T> T getService(String serviceName) {
        return serviceManager.getService(serviceName);
    }

    /**
     * Returns a list of services matching provides service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    @PrivateApi
    public <S> Collection<S> getServices(Class<S> serviceClass) {
        return serviceManager.getServices(serviceClass);
    }

    @PrivateApi
    public Node getNode() {
        return node;
    }

    @PrivateApi
    public ClusterService getClusterService() {
        return node.getClusterService();
    }

    public Cluster getCluster() {
        return getClusterService().getClusterProxy();
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    public final int getPartitionId(Data key) {
        return node.partitionService.getPartitionId(key);
    }

    public PartitionInfo getPartitionInfo(int partitionId) {
        PartitionInfo p = node.partitionService.getPartition(partitionId);
        if (p.getOwner() == null) {
            // probably ownerships are not set yet.
            // force it.
            node.partitionService.getPartitionOwner(partitionId);
        }
        return p;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public Config getConfig() {
        return node.getConfig();
    }

    EventService getEventService() {
        return eventService;
    }

    @PrivateApi
    @Deprecated
    public ExecutorService getEventExecutor() {
        return eventExecutorService;
    }

    public Future<?> submit(Runnable task) {
        return cachedExecutorService.submit(task);
    }

    public void execute(final Runnable command) {
        cachedExecutorService.execute(command);
    }

    public void schedule(final Runnable command, long delay, TimeUnit unit) {
        scheduledExecutorService.schedule(command, delay, unit);
    }

    public void scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public void scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    public Data toData(final Object object) {
        ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
        return IOUtil.toData(object);
    }

    public Object toObject(final Object object) {
        ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
        return IOUtil.toObject(object);
    }

    public TransactionImpl getTransaction() {
        return ThreadContext.get().getTransaction();
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    public GroupProperties getGroupProperties() {
        return node.getGroupProperties();
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
                    throw new PartitionMigratingException(getThisAddress(), partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
                PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                if (op instanceof PartitionLevelOperation) {
                    partitionLock = partitionInfo.getWriteLock();
                    partitionLock.lock();
                } else {
                    partitionLock = partitionInfo.getReadLock();
                    if (!partitionLock.tryLock(500, TimeUnit.MILLISECONDS)) {
                        partitionLock = null;
                        throw new PartitionMigratingException(getThisAddress(), partitionId,
                                op.getClass().getName(), op.getServiceName());
                    }
                    final Address owner = partitionInfo.getReplicaAddress(op.getReplicaIndex());
                    final boolean validatesTarget = op.validatesTarget();
                    if (validatesTarget && !getThisAddress().equals(owner)) {
                        throw new WrongTargetException(getThisAddress(), owner, partitionId,
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
                    waitNotifyService.wait(so);
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
                    waitNotifyService.notify(notifier);
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
        final int maxBackups = getClusterService().getSize() - 1;

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
            final PartitionInfo partitionInfo = getPartitionInfo(partitionId);

            if (syncBackupCount > 0) {
                syncBackups = new ArrayList<Future>(syncBackupCount);
                for (int replicaIndex = 1; replicaIndex <= syncBackupCount; replicaIndex++) {
                    final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                    if (target != null) {
                        if (target.equals(getThisAddress())) {
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
                        if (target.equals(getThisAddress())) {
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
        backupCount = Math.min(getClusterService().getSize() - 1, backupCount);
        if (backupCount > 0) {
            List<Future> backupOps = new ArrayList<Future>(backupCount);
            PartitionInfo partitionInfo = getPartitionInfo(partitionId);
            for (int i = 0; i < backupCount; i++) {
                int replicaIndex = i + 1;
                Address replicaTarget = partitionInfo.getReplicaAddress(replicaIndex);
                if (replicaTarget != null) {
                    if (replicaTarget.equals(getThisAddress())) {
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

    @PrivateApi
    public void shutdown() {
        serviceManager.stopServices();
        cachedExecutorService.shutdown();
        scheduledExecutorService.shutdownNow();
        eventExecutorService.shutdownNow();
        try {
            cachedExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.FINEST, e.getMessage(), e);
        }
        mapCalls.clear();
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
                final Operation op = (Operation) toObject(data);
                op.setNodeService(NodeServiceImpl.this).setCaller(caller);
                op.setConnection(packet.getConn());
                if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
                    op.setResponseHandler(ResponseHandlerFactory.NO_RESPONSE_HANDLER);
                    eventService.onEvent((EventOperation) op);
                } else {
                    ResponseHandlerFactory.setRemoteResponseHandler(NodeServiceImpl.this, op);
                    runOperation(op);
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                send(new ErrorResponse(getThisAddress(), e), packet.getConn());
            }
        }
    }

    private class WaitingOpProcessorImpl implements WaitingOpProcessor {

        public void process(final WaitingOp so) throws Exception {
            cachedExecutorService.execute(new Runnable() {
                public void run() {
                    runOperation(so);
                }
            });
        }

        public void processUnderExistingLock(Operation operation) {
            final NodeServiceImpl nodeService = NodeServiceImpl.this;
            nodeService.runOperationUnderExistingLock(operation);
        }
    }

    private static final ClassLoader thisClassLoader = NodeService.class.getClassLoader();

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

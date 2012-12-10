/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.instance.Node;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.transaction.TransactionImpl;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
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
    private final Lock[] locks = new Lock[100000];
    private final ConcurrentMap<Long, Call> mapCalls = new ConcurrentHashMap<Long, Call>(1000);
    private final AtomicLong localIdGen = new AtomicLong();
    private final ServiceManager serviceManager;
    private final WaitNotifyService waitNotifyService;

    public NodeServiceImpl(Node node) {
        this.node = node;
        logger = node.getLogger(NodeService.class.getName());
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
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
        waitNotifyService = new WaitNotifyService(new WaitNotifyService.WaitingOpProcessor() {
            public void process(final WaitNotifyService.WaitingOp so) throws Exception {
                cachedExecutorService.execute(new Runnable() {
                    public void run() {
                        executeOperation(so);
                    }
                });
            }

            public void processUnderExistingLock(Operation operation) {
                NodeServiceImpl.this.processUnderExistingLock(operation);
            }
        });
    }

    public void start() {
        serviceManager.startServices();
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception {
        final Map<Address, ArrayList<Integer>> memberPartitions = getMemberPartitions();
        final Map<Address, Future> responses = new HashMap<Address, Future>(memberPartitions.size());
        final Data operationData = toData(op); // don't use op object in invocations!
        for (Entry<Address, ArrayList<Integer>> mp : memberPartitions.entrySet()) {
            Address target = mp.getKey();
            Invocation inv = createInvocationBuilder(serviceName, new PartitionIterator(mp.getValue(), operationData),
                    EXECUTOR_THREAD_ID).setTarget(target).setTryCount(5).setTryPauseMillis(300).build();
            Future future = inv.invoke();
            responses.put(target, future);
        }
        final Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(partitionCount);
        for (Entry<Address, Future> response : responses.entrySet()) {
            try {
                Object result = response.getValue().get();
                partitionResults.putAll((Map<Integer, Object>) toObject(result));
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
            if (result instanceof Exception) {
                failedPartitions.add(partitionId);
            }
        }
        for (Integer failedPartition : failedPartitions) {
            Invocation inv = createInvocationBuilder(serviceName,
                    new OperationWrapper(operationData), failedPartition).build();
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

    private Map<Address, ArrayList<Integer>> getMemberPartitions() {
        final int members = node.getClusterService().getSize();
        Map<Address, ArrayList<Integer>> memberPartitions = new HashMap<Address, ArrayList<Integer>>(members);
        for (int i = 0; i < partitionCount; i++) {
            Address owner = node.partitionService.getPartitionOwner(i);
            ArrayList<Integer> ownedPartitions = memberPartitions.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new ArrayList<Integer>();
                memberPartitions.put(owner, ownedPartitions);
            }
            ownedPartitions.add(i);
        }
        return memberPartitions;
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        return new InvocationBuilder(this, serviceName, op, partitionId);
    }

    void invoke(final InvocationImpl inv) {
        final Operation currentOp = (Operation) ThreadContext.get().getCurrentOperation();
        final Operation op = inv.getOperation();
//        if (currentOp != null && !(op instanceof BackupOperation)) {
//            throw new HazelcastException(Thread.currentThread()
//                    + " cannot make remote call: " + inv.getOperation() + " currentOp:" + currentOp);
//        }
        final Address target = inv.getTarget();
        final int partitionId = inv.getPartitionId();
        final int replicaIndex = inv.getReplicaIndex();
        final String serviceName = inv.getServiceName();
        op.setNodeService(this).setServiceName(serviceName).setCaller(getThisAddress())
                .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        checkInvocation(inv);
        if (getThisAddress().equals(target)) {
            ResponseHandlerFactory.setLocalResponseHandler(inv);
            executeOperation(op);
        } else {
            Call call = new Call(target, inv);
            long callId = registerCall(call);
            op.setCallId(callId);
            boolean sent = send(op, partitionId, target);
            if (!sent) {
                inv.setResult(new RetryableException(new IOException("Packet not sent!")));
            }
        }
    }

    private void checkInvocation(InvocationImpl inv) {
        final Address target = inv.getTarget();
        final Operation op = inv.getOperation();
        final int partitionId = inv.getPartitionId();
        final String serviceName = inv.getServiceName();
        if (target == null) {
            throw new WrongTargetException(getThisAddress(), target, partitionId,
                    op.getClass().getName(), serviceName);
        }
        if (!isJoinOperation(op) && getClusterService().getMember(target) == null) {
            throw new TargetNotMemberException(target, partitionId, op.getClass().getName(), serviceName);
        }
    }

    public void runOperation(final Operation op) throws Exception {
        executeOperation(op);
    }

    @PrivateApi
    public void handleOperation(final Packet packet) {
        final Executor executor = cachedExecutorService;
        executor.execute(new RemoteOperationExecutor(packet));
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

    public boolean send(final Operation op, final int partitionId, final int replicaIndex) {
        Address target = getPartitionInfo(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.log(Level.WARNING, "No target available for partition: "
                    + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, partitionId, target);
    }

    public boolean send(final Operation op, final int partitionId, final Address target) {
        if (target == null || getThisAddress().equals(target)) {
            op.setNodeService(this);
            executeOperation(op); // TODO: not sure what to do here...
            return true;
        } else {
            return send(op, partitionId, node.getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final int partitionId, final Connection connection) {
        Data opData = toData(op);
        return node.clusterService.send(new Packet(opData, partitionId, connection), connection);
    }

    @PrivateApi
    public void disconnectExistingCalls(Address deadAddress) {
        for (Call call : mapCalls.values()) {
            call.onDisconnect(deadAddress);
        }
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
//            logger.log(Level.SEVERE, "NO CALL WITH ID: " + callId + ", RESPONSE: " + response,
//                    new HazelcastException());
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

    @PrivateApi
    public ExecutorService getEventService() {
        return eventExecutorService; // ??
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

    private void executeOperation(final Operation op) {
        if (op instanceof KeyBasedOperation) {
            System.out.println("Process without lock " + op);
        }
        final ThreadContext threadContext = ThreadContext.get();
        threadContext.setCurrentOperation(op);
        ResponseHandler responseHandler = op.getResponseHandler();
        Lock partitionLock = null;
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
                    if (!partitionLock.tryLock()) {
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
                    if (!(op instanceof BackupOperation) && op instanceof KeyBasedOperation) {
                        final int hash = ((KeyBasedOperation) op).getKeyHash();
                        keyLock = locks[Math.abs(hash) % locks.length];
                        keyLock.lock();
                    }
                }
            }
            processUnderExistingLock(op);
        } catch (Throwable e) {
            if (e instanceof RetryableException) {
                logger.log(Level.WARNING, "While executing op: " + op + " -> "
                        + e.getClass() + ": " + e.getMessage());
                logger.log(Level.FINEST, e.getMessage(), e);
            } else {
                logger.log(Level.SEVERE, "While executing op: " + op + " -> "
                        + e.getMessage(), e);
            }
            if (responseHandler != null && op.returnsResponse()) {
                responseHandler.sendResponse(e);
            }
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

    void processUnderExistingLock(Operation op) {
        final ThreadContext threadContext = ThreadContext.get();
        final Object parentOperation = threadContext.getCurrentOperation();
        threadContext.setCurrentOperation(op);
        ResponseHandler responseHandler = op.getResponseHandler();
        if (op instanceof KeyBasedOperation) {
            System.out.println("Process Under lock " + op);
        }
        try {
            op.beforeRun();
            if (op instanceof WaitSupport) {
                WaitSupport so = (WaitSupport) op;
                System.out.println(op + " should wait " + so.shouldWait());
                if (so.shouldWait()) {
                    waitNotifyService.wait(so);
                    return;
                }
            }
            op.run();
            if (op instanceof BackupAwareOperation) {
                final Collection<Future> sync = new LinkedList<Future>();
                final Collection<Future> async = new LinkedList<Future>();
                final Object response = sendBackups(op, sync, async);
                waitResponses(sync);
                if (responseHandler != null && op.returnsResponse()) {
                    responseHandler.sendResponse(response);
                }
                waitResponses(async);
            } else {
                if (responseHandler != null && op.returnsResponse()) {
                    responseHandler.sendResponse(op.getResponse());
                }
            }
            op.afterRun();
            if (op instanceof Notifier) {
                waitNotifyService.notify((Notifier) op);
            }
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                logger.log(Level.WARNING, "While executing op: " + op + " -> "
                        + e.getClass() + ": " + e.getMessage());
                logger.log(Level.FINEST, e.getMessage(), e);
            } else {
                logger.log(Level.SEVERE, "While executing op: " + op + " -> "
                        + e.getMessage(), e);
            }
            if (responseHandler != null && op.returnsResponse()) {
                responseHandler.sendResponse(e);
            }
        } finally {
            threadContext.setCurrentOperation(parentOperation);
        }
    }

    private void waitResponses(final Collection<Future> futures) {
        int size = futures.size();
        while (size > 0) {
            for (Future f : futures) {
                if (!f.isDone()) {
                    try {
                        f.get(1, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (f.isDone()) {
                        size--;
                    }
                }
            }
        }
    }

    private void waitResponses2(final Collection<Future> futures) {
        while (futures.size() > 0) {
            final Iterator<Future> iter = futures.iterator();
            while (iter.hasNext()) {
                final Future f = iter.next();
                try {
                    f.get(1, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (f.isDone()) {
                    iter.remove();
                }
            }
        }
    }

    private Object sendBackups(final Operation op, final Collection<Future> syncBackups,
                               final Collection<Future> asyncBackups)
            throws ExecutionException, TimeoutException, InterruptedException {
        final BackupAwareOperation backupAwareOperation = (BackupAwareOperation) op;
        final int maxBackups = getClusterService().getSize() - 1;
        final int syncBackupCount = backupAwareOperation.getSyncBackupCount() > 0
                ? Math.min(maxBackups, backupAwareOperation.getSyncBackupCount()) : 0;
        final int asyncBackupCount = backupAwareOperation.getAsyncBackupCount() > 0
                ? Math.min(maxBackups - syncBackupCount, backupAwareOperation.getAsyncBackupCount()) : 0;
        if (syncBackupCount + asyncBackupCount > 0) {
            Operation backupResponse = null;
            final Operation backupOp = backupAwareOperation.getBackupOperation();
            if (backupOp == null) {
                throw new NullPointerException();
            }
            final String serviceName = op.getServiceName();
            final int partitionId = op.getPartitionId();
            final PartitionInfo partitionInfo = getPartitionInfo(partitionId);
            for (int replicaIndex = 1; replicaIndex <= syncBackupCount; replicaIndex++) {
                final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                if (target != null) {
                    if (target.equals(getThisAddress())) {
                        throw new IllegalStateException("Normally shouldn't happen!!");
                    } else {
                        if (op.returnsResponse() && target.equals(op.getCaller())) {
                            backupOp.setServiceName(serviceName).setReplicaIndex(replicaIndex).setPartitionId(partitionId);
                            backupResponse = backupOp;
                        } else {
                            syncBackups.add(createInvocationBuilder(serviceName, backupOp, partitionId)
                                    .setReplicaIndex(replicaIndex).build().invoke());
                        }
                    }
                }
            }
            for (int replicaIndex = syncBackupCount + 1; replicaIndex <= asyncBackupCount; replicaIndex++) {
                final Address target = partitionInfo.getReplicaAddress(replicaIndex);
                if (target != null) {
                    if (target.equals(getThisAddress())) {
                        throw new IllegalStateException("Normally shouldn't happen!!");
                    } else {
                        asyncBackups.add(createInvocationBuilder(serviceName, backupOp, partitionId)
                                .setReplicaIndex(replicaIndex).build().invoke());
                    }
                }
            }
            return op.returnsResponse()
                    ? (backupResponse == null ? op.getResponse() : new MultiResponse(backupResponse, op.getResponse()))
                    : null;
        } else {
            return op.returnsResponse() ? op.getResponse() : null;
        }
    }

    private class RemoteOperationExecutor implements Runnable {
        private final Packet packet;

        private RemoteOperationExecutor(final Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final int partitionId = packet.getPartitionId();
            final Data data = packet.getValue();
            final Address caller = packet.getConn().getEndPoint();
            try {
                final Operation op = (Operation) toObject(data);
                op.setNodeService(NodeServiceImpl.this).setCaller(caller).setPartitionId(partitionId);
                op.setConnection(packet.getConn());
                ResponseHandlerFactory.setRemoteResponseHandler(NodeServiceImpl.this, op);
                executeOperation(op);
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                send(new ErrorResponse(getThisAddress(), e), EXECUTOR_THREAD_ID, packet.getConn());
            }
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
}

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

package com.hazelcast.impl.spi;

import com.hazelcast.impl.cluster.ClusterService;
import com.hazelcast.impl.cluster.JoinOperation;
import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.impl.*;
import com.hazelcast.impl.map.GenericBackupOperation;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.spi.annotation.PrivateApi;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class NodeServiceImpl implements NodeService {

    private final ConcurrentMap<String, Object> services = new ConcurrentHashMap<String, Object>(10);
    private final Workers workers;
    private final ExecutorService executorService;
    private final ExecutorService eventExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Node node;
    private final ILogger logger;
    private final int partitionCount;
    private final ThreadGroup partitionThreadGroup;
    private final ConcurrentMap<Long, Call> mapCalls = new ConcurrentHashMap<Long, Call>(1000);
    private final AtomicLong localIdGen = new AtomicLong();

    public NodeServiceImpl(Node node) {
        this.node = node;
        logger = node.getLogger(NodeService.class.getName());
        final ClassLoader classLoader = node.getConfig().getClassLoader();
        executorService = new ThreadPoolExecutor(
                3, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue(),
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("cached"), classLoader));
        eventExecutorService = Executors.newSingleThreadExecutor(
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("event"), node.getConfig().getClassLoader()));
        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                new ExecutorThreadFactory(node.threadGroup,
                        node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("scheduled"), classLoader));
        partitionThreadGroup = new ThreadGroup(node.threadGroup, "partitionThreads");
        workers = new Workers(partitionThreadGroup, "workers", 2);
        partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception {
        Map<Address, ArrayList<Integer>> memberPartitions = getMemberPartitions();
        List<Future> responses = new ArrayList<Future>(memberPartitions.size());
        Data data = toData(op);
        for (Map.Entry<Address, ArrayList<Integer>> mp : memberPartitions.entrySet()) {
            Address target = mp.getKey();
            Invocation inv = createSingleInvocation(serviceName, new PartitionIterator(mp.getValue(), data), EXECUTOR_THREAD_ID)
                    .setTarget(target).setTryCount(100).setTryPauseMillis(500).build();
            Future future = inv.invoke();
            responses.add(future);
        }
        Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(partitionCount);
        for (Future r : responses) {
            Object result = r.get();
            Map<Integer, Object> partialResult = null;
            if (result instanceof Data) {
                partialResult = (Map<Integer, Object>) toObject(result);
            } else {
                partialResult = (Map<Integer, Object>) result;
            }
            partitionResults.putAll(partialResult);
        }
        List<Integer> failedPartitions = new ArrayList<Integer>(0);
        for (Map.Entry<Integer, Object> partitionResult : partitionResults.entrySet()) {
            int partitionId = partitionResult.getKey();
            Object result = partitionResult.getValue();
            if (result instanceof Exception) {
                failedPartitions.add(partitionId);
            }
        }
        Thread.sleep(500);
        for (Integer failedPartition : failedPartitions) {
            Invocation inv = createSingleInvocation(serviceName, op, failedPartition).build();
            inv.invoke();
            partitionResults.put(failedPartition, inv);
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

    public SingleInvocationBuilder createSingleInvocation(String serviceName, Operation op, int partitionId) {
        return new SingleInvocationBuilder(NodeServiceImpl.this, serviceName, op, partitionId);
    }

    void invokeSingle(final SingleInvocation inv) {
        if (Thread.currentThread().getThreadGroup() == partitionThreadGroup) {
            throw new RuntimeException(Thread.currentThread()
                    + " cannot make another call: "
                    + inv.getOperation()
                    + " currentOp:" + ThreadContext.get().getCurrentOperation());
        }
        final Address target = inv.getTarget();
        final Operation op = inv.getOperation();
        final int partitionId = inv.getPartitionId();
        final int replicaIndex = inv.getReplicaIndex();
        final String serviceName = inv.getServiceName();
        setOperationContext(op, serviceName, node.getThisAddress(), -1, partitionId, replicaIndex);
        checkInvocation(inv);
        if (getThisAddress().equals(target)) {
            ResponseHandlerFactory.setLocalResponseHandler(inv);
            runLocally(op);
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

    private void checkInvocation(SingleInvocation inv) {
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

    public void runLocally(final Operation op) {
        final int partitionId = op.getPartitionId();
        final ExecutorService executor = getExecutor(partitionId);
        executor.execute(new OperationExecutor(op));
    }

    @PrivateApi
    public void handleOperation(final SimpleSocketWritable ssw) {
        final int partitionId = ssw.getPartitionId();
        final Executor executor = getExecutor(partitionId);
        executor.execute(new RemoteOperationExecutor(ssw));
    }

    @PrivateApi
    public Operation setOperationContext(Operation op, String serviceName, Address caller,
                                         long callId, int partitionId, int replicaIndex) {
        op.setNodeService(this)
                .setServiceName(serviceName)
                .setCaller(caller)
                .setCallId(callId)
                .setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex);
        return op;
    }

    public void takeBackups(String serviceName, Operation op, int partitionId, int backupCount, int timeoutSeconds)
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
                    } else {
                        backupOps.add(createSingleInvocation(serviceName, op, partitionId).setReplicaIndex(replicaIndex).build().invoke());
                    }
                }
            }
            for (Future backupOp : backupOps) {
                backupOp.get(timeoutSeconds, TimeUnit.SECONDS);
            }
        }
    }

    public void sendBackups(String serviceName, GenericBackupOperation op, int partitionId, int backupCount) {
        op.setServiceName(serviceName);
        backupCount = Math.min(getClusterService().getSize() - 1, backupCount);
        if (backupCount > 0) {
            Data opData = toData(op);
            PartitionInfo partitionInfo = getPartitionInfo(partitionId);
            for (int i = 0; i < backupCount; i++) {
                int replicaIndex = i + 1;
                Address replicaTarget = partitionInfo.getReplicaAddress(replicaIndex);
                if (replicaTarget != null) {
                    if (replicaTarget.equals(getThisAddress())) {
                        // Normally shouldn't happen!!
                    } else {
                        SimpleSocketWritable ssw = new SimpleSocketWritable(opData, -1, partitionId, replicaIndex, null);
                        node.clusterService.send(ssw, replicaTarget);
                    }
                }
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
            op.run();
            return true;
        } else {
            return send(op, partitionId, node.getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final int partitionId, final Connection connection) {
        Data opData = toData(op);
        return node.clusterService.send(new SimpleSocketWritable(opData, op.getCallId(), partitionId,
                op.getReplicaIndex(), null), connection);
    }

    private ExecutorService getExecutor(int partitionId) {
        if (partitionId >= 0) {
            return workers.getExecutor(partitionId);
        } else if (partitionId == EXECUTOR_THREAD_ID) {
            return executorService;
        } else if (partitionId == EVENT_THREAD_ID) {
            return eventExecutorService;
        } else {
            throw new IllegalArgumentException("Illegal partition id: " + partitionId);
        }
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
    void notifyCall(long callId, Response response) {
        Call call = deregisterRemoteCall(callId);
        if (call != null) {
            call.offerResponse(response);
        }
    }

    public void registerService(String serviceName, Object service) {
        services.put(serviceName, service);
        if (service instanceof ManagedService) {
            ((ManagedService) service).init(this);
        }
    }

    public <T> T getService(String serviceName) {
        return (T) services.get(serviceName);
    }

    @PrivateApi
    public Collection<Object> getServices() {
        return Collections.unmodifiableCollection(services.values());
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
        return eventExecutorService;
    }

    public Future<?> submit(Runnable task) {
        return executorService.submit(task);
    }

    public void execute(final Runnable command) {
        executorService.execute(command);
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
        for (Object service : getServices()) {
            if (service instanceof ManagedService) {
                try {
                    ((ManagedService) service).destroy();
                } catch (Throwable t) {
                    logger.log(Level.SEVERE, "Error while stopping service: " + t.getMessage(), t);
                }
            }
        }
        workers.shutdownNow();
        executorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
        eventExecutorService.shutdownNow();
        try {
            scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        try {
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        workers.awaitTermination(3);
        mapCalls.clear();
    }

    private class Workers {
        private final ThreadGroup partitionThreadGroup;
        final int threadCount;
        final ExecutorService[] workers;
        final AtomicInteger threadNumber = new AtomicInteger();

        Workers(ThreadGroup partitionThreadGroup, String threadName, int threadCount) {
            this.partitionThreadGroup = partitionThreadGroup;
            this.threadCount = threadCount;
            workers = new ExecutorService[threadCount];
            for (int i = 0; i < threadCount; i++) {
                workers[i] = newSingleThreadExecutorService(threadName);
            }
        }

        public ExecutorService getExecutor(int partitionId) {
            return workers[partitionId % threadCount];
        }

        ExecutorService newSingleThreadExecutorService(final String threadName) {
            return new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ExecutorThreadFactory(partitionThreadGroup, node.hazelcastInstance, node.getThreadPoolNamePrefix(threadName),
                            threadNumber, node.getConfig().getClassLoader()),
                    new RejectedExecutionHandler() {
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        }
                    }
            );
        }

        void shutdownNow() {
            for (ExecutorService worker : workers) {
                worker.shutdownNow();
            }
        }

        void awaitTermination(int timeoutSeconds) {
            for (ExecutorService worker : workers) {
                try {
                    worker.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private class OperationExecutor implements Runnable {
        protected Operation op;

        private OperationExecutor() {
        }

        private OperationExecutor(final Operation op) {
            this.op = op;
        }

        public void run() {
            ThreadContext.get().setCurrentOperation(op);
//                System.err.println(Thread.currentThread().getName() + " executing " +  op.getClass().getName());
            try {
                checkOperation(op);
                op.run();
            } catch (Throwable e) {
                handleOperationError(op, e);
            }
        }

        public void setOp(final Operation op) {
            this.op = op;
        }

        private void checkOperation(final Operation op) {
            final int partitionId = op.getPartitionId();
            if (partitionId >= 0) {
                PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                Address owner = partitionInfo.getReplicaAddress(op.getReplicaIndex());
                if (!isPartitionLockFreeOperation(op) && node.partitionService.isPartitionLocked(partitionId)) {
                    throw new PartitionLockedException(getThisAddress(), owner, partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
                final boolean shouldValidateTarget = op.shouldValidateTarget();
                if (shouldValidateTarget && !getThisAddress().equals(owner)) {
                    throw new WrongTargetException(getThisAddress(), owner, partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
            }
        }

        private void handleOperationError(final Operation op, final Throwable e) {
            if (e instanceof RetryableException) {
                logger.log(Level.WARNING, e.getClass() + ": " + e.getMessage());
                logger.log(Level.FINEST, e.getMessage(), e);
            } else {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
            final ResponseHandler responseHandler = op.getResponseHandler();
            if (responseHandler != null) {
                responseHandler.sendResponse(e);
            }
        }

    }

    private class RemoteOperationExecutor extends OperationExecutor implements Runnable {
        private final SimpleSocketWritable ssw;

        private RemoteOperationExecutor(final SimpleSocketWritable ssw) {
            this.ssw = ssw;
        }

        public void run() {
            final int partitionId = ssw.getPartitionId();
            final int replicaIndex = ssw.getReplicaIndex();
            final Data data = ssw.getValue();
            final long callId = ssw.getCallId();
            final Address caller = ssw.getConn().getEndPoint();

            try {
                setOp((Operation) toObject(data));
                setOperationContext(op, op.getServiceName(), caller, callId, partitionId, replicaIndex);
                op.setConnection(ssw.getConn());
                ResponseHandlerFactory.setRemoteResponseHandler(NodeServiceImpl.this, op, partitionId, callId);
                super.run();
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                node.clusterService.send(new SimpleSocketWritable(toData(e), callId,
                        partitionId, replicaIndex, null), ssw.getConn());
            }
        }
    }

    private static final ClassLoader thisClassLoader = NodeService.class.getClassLoader();

    private static boolean isPartitionLockFreeOperation(Operation op) {
        return op instanceof PartitionLockFreeOperation
               && op.getClass().getClassLoader() == thisClassLoader;
    }

    private static boolean isJoinOperation(Operation op) {
        return op instanceof JoinOperation
               && op.getClass().getClassLoader() == thisClassLoader;
    }
}

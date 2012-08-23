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

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Member;
import com.hazelcast.impl.ExecutorThreadFactory;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.map.GenericBackupOperation;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.SimpleSocketWritable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.REMOTE_CALL;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class NodeService {

    private final ConcurrentMap<String, Object> services = new ConcurrentHashMap<String, Object>(10);
    private final ExecutorService executorService;
    private final Workers blockingWorkers;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Node node;
    private final ILogger logger;
    private final int partitionCount;
    private final int maxBackupCount;

    public NodeService(Node node) {
        this.node = node;
        logger = node.getLogger(NodeService.class.getName());
        final ClassLoader classLoader = node.getConfig().getClassLoader();
        executorService = new ThreadPoolExecutor(
                3, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue(),
                new ExecutorThreadFactory(node.threadGroup, node.getThreadPoolNamePrefix("cached"), classLoader));
        scheduledExecutorService = new ScheduledThreadPoolExecutor(2, new ExecutorThreadFactory(node.threadGroup,
                node.getThreadPoolNamePrefix("scheduled"), classLoader));
        blockingWorkers = new Workers("hz.Blocking", 2);
        partitionCount = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        maxBackupCount = MapConfig.MAX_BACKUP_COUNT;
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception {
        Map<Member, ArrayList<Integer>> memberPartitions = getMemberPartitions();
        List<Future> responses = new ArrayList<Future>(memberPartitions.size());
        Data data = toData(op);
        for (Map.Entry<Member, ArrayList<Integer>> mp : memberPartitions.entrySet()) {
            Address target = ((MemberImpl) mp.getKey()).getAddress();
            Invocation inv = createSingleInvocation(serviceName, new PartitionIterator(mp.getValue(), data), -1)
                    .setTarget(target).setTryCount(100).setTryPauseMillis(500).build();
            Future future = inv.invoke();
            responses.add(future);
        }
        Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(partitionCount);
        for (Future r : responses) {
            Object result = r.get();
            Map<Integer, Object> partialResult = null;
            if (result instanceof Data) {
                partialResult = (Map<Integer, Object>) toObject((Data) r.get());
            } else {
                partialResult = (Map<Integer, Object>) result;
            }
            System.out.println(partialResult);
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

    private Map<Member, ArrayList<Integer>> getMemberPartitions() {
        Set<Member> members = node.getClusterImpl().getMembers();
        Map<Member, ArrayList<Integer>> memberPartitions = new HashMap<Member, ArrayList<Integer>>(members.size());
        for (int i = 0; i < partitionCount; i++) {
            Member owner = getOwner(i);
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
        return new SingleInvocationBuilder(NodeService.this, serviceName, op, partitionId);
    }

    void invokeSingle(final SingleInvocation inv) {
//        if (Thread.currentThread().getName().indexOf("hz.Blocking") != -1) {
//            throw new RuntimeException(Thread.currentThread() + " cannot make another call! " + inv.getOperation());
//        }
        ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
        final Address target = inv.getTarget();
        final Operation op = inv.getOperation();
        final int partitionId = inv.getPartitionId();
        final int replicaIndex = inv.getReplicaIndex();
        final String serviceName = inv.getServiceName();
        final boolean nonBlocking = (op instanceof NonBlockingOperation);
        setOperationContext(op, serviceName, node.getThisAddress(), -1, partitionId, replicaIndex);
        if (target == null) {
            throw new NullPointerException(inv.getOperation() + ": Target is null");
        }
        if (!(op instanceof NonMemberOperation) && getClusterImpl().getMember(target) == null) {
            throw new TargetNotMemberException(target, partitionId, op.getClass().getName(), serviceName);
        }
        if (getThisAddress().equals(target)) {
            if (!(op instanceof NoReply)) {
                op.setResponseHandler(new ResponseHandler() {
                    public void sendResponse(Object obj) {
                        inv.setResult(obj);
                    }
                });
            }
            runLocally(partitionId, op);
        } else {
            Data valueData = toData(op);
            TheCall call = new TheCall(target, inv);
            long callId = node.clusterImpl.registerCall(call);
            SimpleSocketWritable ssw = new SimpleSocketWritable(REMOTE_CALL, serviceName, valueData, callId, partitionId, replicaIndex, null, nonBlocking);
            boolean sent = node.clusterImpl.send(ssw, target);
            if (!sent) {
                inv.setResult(new IOException("Packet not sent!"));
            }
        }
    }

    void runLocally(final int partitionId, final Operation op) {
        final boolean nonBlocking = op instanceof NonBlockingOperation;
        final ExecutorService executor = getExecutor(partitionId);
        executor.execute(new Runnable() {
            public void run() {
                if (partitionId != -1 && !nonBlocking) {
                    PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                    Address owner = partitionInfo.getOwner();
                    if (!getThisAddress().equals(owner)) {
                        op.getResponseHandler().sendResponse(
                                new WrongTargetException(getThisAddress(), owner, partitionId, op.getClass().getName()));
                        return;
                    }
                }
                try {
                    op.run();
                } catch (Throwable e) {
                    e.printStackTrace();
                    op.getResponseHandler().sendResponse(e);
                }
            }
        });
    }

    public void handleOperation(final SimpleSocketWritable ssw) {
        ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
        final int partitionId = ssw.getPartitionId();
        final int replicaIndex = ssw.getReplicaIndex();
        final Data data = ssw.getValue();
        final long callId = ssw.getCallId();
        final Address caller = ssw.getConn().getEndPoint();
        final String serviceName = ssw.getName();
        final Executor executor = getExecutor(partitionId);
        executor.execute(new Runnable() {
            public void run() {
                try {
                    ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
                    final Operation op = (Operation) toObject(data);
                    setOperationContext(op, op.getServiceName(), caller, callId, partitionId, replicaIndex);
                    op.setConnection(ssw.getConn());
                    final boolean noReply = (op instanceof NoReply);
                    if (!noReply) {
                        ResponseHandler responseHandler = new ResponseHandler() {
                            public void sendResponse(Object response) {
                                if (!(response instanceof Operation)) {
                                    response = new Response(response);
                                }
                                boolean nb = (response instanceof NonBlockingOperation);
                                node.clusterImpl.send(new SimpleSocketWritable(REMOTE_CALL, serviceName, toData(response), callId, partitionId, replicaIndex, null, nb), ssw.getConn());
                            }
                        };
                        op.setResponseHandler(responseHandler);
                    }
                    try {
                        if (partitionId != -1) {
                            PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                            Address owner = partitionInfo.getOwner();
                            if (!(op instanceof NonBlockingOperation) && !getThisAddress().equals(owner)) {
                                throw new WrongTargetException(getThisAddress(), owner, partitionId,
                                        op.getClass().getName(), serviceName);
                            }
                        }
                        op.run();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        if (e instanceof RetryableException) {
                            logger.log(Level.WARNING, e.getClass() + ": " + e.getMessage());
                            logger.log(Level.FINEST, e.getMessage(), e);
                        } else {
                            logger.log(Level.SEVERE, e.getMessage(), e);
                        }
                        if (!noReply) {
                            op.getResponseHandler().sendResponse(e);
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                    node.clusterImpl.send(new SimpleSocketWritable(REMOTE_CALL, serviceName, toData(e), callId, partitionId, replicaIndex, null, true), ssw.getConn());
                }
            }
        });
    }

    private Operation setOperationContext(Operation op, String serviceName, Address caller,
                                          long callId, int partitionId, int replicaIndex) {
        op.setNodeService(this)
                .setServiceName(serviceName)
                .setCaller(caller)
                .setCallId(callId)
                .setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex);
        return op;
    }

    public void takeBackups(String serviceName, Operation op, int partitionId, int backupCount, int timeoutSeconds) throws ExecutionException, TimeoutException, InterruptedException {
        op.setServiceName(serviceName);
        backupCount = Math.min(getClusterImpl().getSize() - 1, backupCount);
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

    public void sendBackups(String serviceName, GenericBackupOperation op, int partitionId, int backupCount) throws ExecutionException, TimeoutException, InterruptedException {
        op.setServiceName(serviceName);
        backupCount = Math.min(getClusterImpl().getSize() - 1, backupCount);
        if (backupCount > 0) {
            Data opData = toData(op);
            PartitionInfo partitionInfo = getPartitionInfo(partitionId);
            for (int i = 0; i < backupCount; i++) {
                int replicaIndex = i + 1;
                Address replicaTarget = partitionInfo.getReplicaAddress(replicaIndex);
                System.out.println(partitionInfo);
                if (replicaTarget != null) {
                    if (replicaTarget.equals(getThisAddress())) {
                        // Normally shouldn't happen!!
                    } else {
                        SimpleSocketWritable ssw = new SimpleSocketWritable(REMOTE_CALL, serviceName, opData, -1, partitionId, replicaIndex, null, true);
                        node.clusterImpl.send(ssw, replicaTarget);
                        System.out.println(getThisAddress() + " sending backup to " + replicaTarget + " and backupId: " + op.getFirstCallerId());
                    }
                } else {
                    System.out.println(getThisAddress() + " NOT sending backup target null and backupId: " + op.getFirstCallerId());
                }
            }
        }
    }

    public void printPartitions() {
        for (int i = 0; i < 271; i++) {
            System.out.println(getPartitionInfo(i));
        }
    }

    public void send(String serviceName, Operation op, int partitionId, int replicaIndex, long callId, Address target) {
        try {
            op.setServiceName(serviceName);
            if (target == null || getThisAddress().equals(target)) {
                setOperationContext(op, serviceName, getThisAddress(), callId, partitionId, replicaIndex);
                op.run();
            } else {
                Data opData = toData(op);
                node.clusterImpl.send(new SimpleSocketWritable(REMOTE_CALL, serviceName, opData, callId, partitionId, replicaIndex, null, true), target);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ExecutorService getExecutor(int partitionId) {
        if (partitionId > -1) {
            return blockingWorkers.getExecutor(partitionId);
        } else {
            return executorService;
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    class Workers {
        final int threadCount;
        final ExecutorService[] workers;

        Workers(String threadNamePrefix, int threadCount) {
            this.threadCount = threadCount;
            workers = new ExecutorService[threadCount];
            for (int i = 0; i < threadCount; i++) {
                workers[i] = newSingleThreadExecutorService(threadNamePrefix);
            }
        }

        public ExecutorService getExecutor(int partitionId) {
            return workers[partitionId % threadCount];
        }

        ExecutorService newSingleThreadExecutorService(final String threadName) {
            return new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ExecutorThreadFactory(node.threadGroup, node.getThreadNamePrefix(threadName),
                            node.getConfig().getClassLoader()) {
                        protected void beforeRun() {
                            ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
                        }
                    },
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

    public void notifyCall(long callId, Response response) {
        TheCall call = (TheCall) node.clusterImpl.deregisterRemoteCall(callId);
        if (call != null) {
            call.offerResponse(response);
        }
    }

    public void registerService(String serviceName, Object obj) {
        services.put(serviceName, obj);
    }

    public <T> T getService(String serviceName) {
        return (T) services.get(serviceName);
    }

    public <T> Collection<T> getServices(final Class<T> type) {
        final Collection<T> result = new LinkedList<T>();
        for (Object service : services.values()) {
            if (type.isAssignableFrom(service.getClass())) {
                result.add((T) service);
            }
        }
        return result;
    }

    public Node getNode() {
        return node;
    }

    public ClusterImpl getClusterImpl() {
        return node.getClusterImpl();
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    Member getOwner(int partitionId) {
        return node.partitionManager.partitionServiceImpl.getPartition(partitionId).getOwner();
    }

    public final int getPartitionId(Data key) {
        return node.partitionManager.getPartitionId(key);
    }

    PartitionInfo getPartitionInfo(int partitionId) {
        PartitionInfo p = node.partitionManager.getPartition(partitionId);
        if (p.getOwner() == null) {
            // probably ownerships are not set yet.
            // force it.
            getOwner(partitionId);
        }
        return p;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public void shutdown() {
        blockingWorkers.shutdownNow();
        executorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
        try {
            scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        try {
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        blockingWorkers.awaitTermination(3);
    }
}

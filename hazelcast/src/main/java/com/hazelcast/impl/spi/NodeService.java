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
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static com.hazelcast.impl.ClusterOperation.REMOTE_CALL;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class NodeService {

    private final ConcurrentMap<String, Object> services = new ConcurrentHashMap<String, Object>(10);
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Workers nonBlockingWorkers = new Workers("hz.NonBlocking", 1);
    private final Workers blockingWorkers = new Workers("hz.Blocking", 8);
    private final Node node;
    final int PARTITION_COUNT;
    final int MAX_BACKUP_COUNT;

    public NodeService(Node node) {
        this.node = node;
        this.PARTITION_COUNT = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        this.MAX_BACKUP_COUNT = MapConfig.MAX_BACKUP_COUNT;
    }

    public final int getPartitionId(Data key) {
        return node.concurrentMapManager.getPartitionId(key);
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception {
        Map<Member, ArrayList<Integer>> memberPartitions = getMemberPartitions();
        List<Future> responses = new ArrayList<Future>(memberPartitions.size());
        Data data = toData(op);
        for (Map.Entry<Member, ArrayList<Integer>> mp : memberPartitions.entrySet()) {
            Address target = ((MemberImpl) mp.getKey()).getAddress();
            responses.add(invoke(serviceName, new PartitionIterator(mp.getValue(), data), target, -1));
        }
        Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(PARTITION_COUNT);
        for (Future response : responses) {
            Response r = (Response) response.get();
            Map<Integer, Object> partialResult = (Map<Integer, Object>) r.getResult();
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
        System.out.println("TRYING AGAIN...");
        for (Integer failedPartition : failedPartitions) {
            partitionResults.put(failedPartition, invoke(serviceName, op, getPartitionInfo(failedPartition).getOwner(), failedPartition));
        }
        for (Integer failedPartition : failedPartitions) {
            Future f = (Future) partitionResults.get(failedPartition);
            Object result = f.get();
            System.out.println(failedPartition + " now response " + result);
            partitionResults.put(failedPartition, result);
        }
        return partitionResults;
    }

    public Member getOwner(int partitionId) {
        return node.concurrentMapManager.getPartitionServiceImpl().getPartition(partitionId).getOwner();
    }

    private Map<Member, ArrayList<Integer>> getMemberPartitions() {
        Set<Member> members = node.getClusterImpl().getMembers();
        Map<Member, ArrayList<Integer>> memberPartitions = new HashMap<Member, ArrayList<Integer>>(members.size());
        for (int i = 0; i < PARTITION_COUNT; i++) {
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

    public Future invokeOptimistically(String serviceName, Operation op, int partitionId) {
        return invoke(serviceName, op, partitionId, 0, 10, 500);
    }

    public Future invokeOptimistically(String serviceName, Operation op, int partitionId, int replicaIndex) {
        return invoke(serviceName, op, partitionId, replicaIndex, 10, 500);
    }

    public Future invoke(String serviceName, Operation op, int partitionId, int tryCount, long tryPauseMillis) {
        return invoke(serviceName, op, partitionId, 0, tryCount, tryPauseMillis);
    }

    public Future invoke(String serviceName, Operation op, int partitionId, int replicaIndex, int tryCount, long tryPauseMillis) {
        SinglePartitionInvocation inv = new SinglePartitionInvocation(serviceName, op, getPartitionInfo(partitionId), replicaIndex, tryCount, tryPauseMillis);
        invokeOnSinglePartition(inv);
        return inv;
    }

    interface Invocation extends Future {
        void invoke();
    }

    class SinglePartitionInvocation extends FutureTask implements Invocation, Callback {
        private final String serviceName;
        private final Operation op;
        private final PartitionInfo partitionInfo;
        private final int replicaIndex;
        private final int tryCount;
        private final long tryPauseMillis;
        private volatile int invokeCount = 0;

        SinglePartitionInvocation(String serviceName, Operation op, PartitionInfo partitionInfo, int replicaIndex, int tryCount, long tryPauseMillis) {
            super(op);
            this.serviceName = serviceName;
            this.op = op;
            this.partitionInfo = partitionInfo;
            this.replicaIndex = replicaIndex;
            this.tryCount = tryCount;
            this.tryPauseMillis = tryPauseMillis;
        }

        public void notify(Operation op, Object result) {
            if (result instanceof Response) {
                Response response = (Response) result;
                if (response.isException()) {
                    setResult(response.getResult());
                } else {
                    setResult(response.getResultData());
                }
            } else {
                setResult(result);
            }
        }

        public void run() {
            try {
                Object result = op.call();
                notify(op, result);
            } catch (Throwable e) {
                setResult(e);
            }
        }

        public Address getTarget() {
            return partitionInfo.getReplicaAddress(replicaIndex);
        }

        public void invoke() {
            try {
                invokeCount++;
                invokeOnSinglePartition(SinglePartitionInvocation.this);
            } catch (Exception e) {
                setResult(e);
            }
        }

        public void setResult(Object obj) {
            if (obj instanceof RetryableException) {
                if (invokeCount < tryCount) {
                    try {
                        Thread.sleep(tryPauseMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    invoke();
                } else {
                    setException((Throwable) obj);
                }
            } else {
                if (obj instanceof Exception) {
                    setException((Throwable) obj);
                } else {
                    set(obj);
                }
            }
        }

        public String getServiceName() {
            return serviceName;
        }

        public Operation getOperation() {
            return op;
        }

        public PartitionInfo getPartitionInfo() {
            return partitionInfo;
        }

        public int getReplicaIndex() {
            return replicaIndex;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        public boolean isCancelled() {
            return false;
        }
    }

    public Future invoke(String serviceName, Operation op, Address target, int partitionId) throws Exception {
        if (target == null) {
            throw new NullPointerException(op + ": Target is null");
        }
        if (getClusterImpl().getMember(target) == null) {
            throw new RuntimeException("Target not a member: " + target);
        }
        if (getThisAddress().equals(target)) {
            return invokeLocally(serviceName, op, partitionId);
        } else {
            return invokeRemotely(serviceName, target, op, partitionId);
        }
    }

    void invokeOnSinglePartition(final SinglePartitionInvocation inv) {
        final Address target = inv.getTarget();
        final Operation op = inv.getOperation();
        final int partitionId = inv.getPartitionInfo().getPartitionId();
        final String serviceName = inv.getServiceName();
        final boolean nonBlocking = (op instanceof NonBlockingOperation);
        setOperationContext(op, serviceName, node.getThisAddress(), -1, partitionId)
                .setLocal(true);
        if (target == null) {
            throw new NullPointerException(inv.getOperation() + ": Target is null");
        }
        if (getClusterImpl().getMember(target) == null) {
            throw new RuntimeException("Target not a member: " + target);
        }
        if (getThisAddress().equals(target)) {
            final ExecutorService executor = getExecutor(partitionId, nonBlocking);
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        if (partitionId != -1 && !nonBlocking) {
                            PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                            Address owner = partitionInfo.getOwner();
                            if (!getThisAddress().equals(owner)) {
                                throw new WrongTargetException(getThisAddress(), owner);
                            }
                        }
                        inv.run();
                    } catch (Throwable e) {
                        inv.setResult(e);
                    }
                }
            });
        } else {
            if (getThisAddress().equals(target)) {
                throw new RuntimeException("RemoteTarget cannot be local!");
            }
            final Packet packet = new Packet();
            packet.operation = REMOTE_CALL;
            packet.blockId = partitionId;
            packet.name = serviceName;
            packet.longValue = nonBlocking ? 1 : 0;
            Data valueData = toData(op);
            packet.setValue(valueData);
            TheCall call = new TheCall(target, op, inv);
            boolean sent = node.concurrentMapManager.registerAndSend(target, packet, call);
            if (!sent) {
                inv.setResult(new IOException());
            }
        }
    }

    private Future invokeLocally(String serviceName, Operation op, int partitionId) {
        return invokeLocally(serviceName, partitionId, op);
    }

    private Future invokeLocally(String serviceName, int partitionId, Operation op) {
        setOperationContext(op, serviceName, node.getThisAddress(), -1, partitionId)
                .setLocal(true);
        return runLocally(partitionId, op, (op instanceof NonBlockingOperation));
    }

    private Future invokeRemotely(String serviceName, Address target, Operation op, int partitionId) throws Exception {
        return invokeRemotely(serviceName, target, op, partitionId, null);
    }

    public Future runLocally(final int partitionId, final Callable op, final boolean nonBlocking) {
        final ExecutorService executor = getExecutor(partitionId, nonBlocking);
        return executor.submit(new Callable() {
            public Object call() throws Exception {
                Object response = null;
                if (partitionId != -1 && !(op instanceof NonBlockingOperation)) {
                    PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                    Address owner = partitionInfo.getOwner();
                    if (!getThisAddress().equals(owner)) {
                        response = new WrongTargetException(getThisAddress(), owner);
                    }
                } else {
                    response = op.call();
                }
                return response;
            }
        });
    }

    private Future invokeRemotely(String serviceName, Address target, Operation op, int partitionId, Callback callback) throws Exception {
        if (target == null) throw new NullPointerException("Target is null");
        if (node.getClusterImpl().getMember(target) == null) {
            throw new RuntimeException("TargetNotClusterMember[" + target + "]");
        }
        if (getThisAddress().equals(target)) {
            throw new RuntimeException("RemoteTarget cannot be local!");
        }
        final Packet packet = new Packet();
        packet.operation = REMOTE_CALL;
        packet.blockId = partitionId;
        packet.name = serviceName;
        packet.longValue = (op instanceof NonBlockingOperation) ? 1 : 0;
        Data valueData = toData(op);
        packet.setValue(valueData);
        TheCall call = new TheCall(target, op, callback);
        boolean sent = node.concurrentMapManager.registerAndSend(target, packet, call);
        if (!sent) {
            throw new IOException();
        }
        return null;
    }

    public void handleOperation(final Packet packet) {
        final int partitionId = packet.blockId;
        final boolean backup = packet.longValue == 1;
        final Data data = packet.getValueData();
        final long callId = packet.callId;
        final Address caller = packet.conn.getEndPoint();
        final String serviceName = packet.name;
        final Executor executor = getExecutor(partitionId, backup);
        executor.execute(new Runnable() {
            public void run() {
                try {
                    Object response = null;
                    final Operation op = (Operation) toObject(data);
                    PartitionInfo partitionInfo = getPartitionInfo(partitionId);
                    Address owner = partitionInfo.getOwner();
                    if (partitionId != -1 && !(op instanceof NonBlockingOperation) && !getThisAddress().equals(owner)) {
                        response = new Response(new WrongTargetException(getThisAddress(), owner), true);
                    } else {
                        try {
                            setOperationContext(op, serviceName, caller, callId, partitionId);
                            response = op.call();
                        } catch (Throwable e) {
                            e.printStackTrace();
                            response = e;
                        }
                    }
                    if (!(op instanceof NoReply)) {
                        if (!(response instanceof Operation)) {
                            response = new Response(response);
                        }
                        packet.clearForResponse();
                        packet.blockId = partitionId;
                        packet.callId = callId;
                        packet.longValue = (response instanceof NonBlockingOperation) ? 1 : 0;
                        packet.setValue(toData(response));
                        packet.name = serviceName;
                        node.concurrentMapManager.sendOrReleasePacket(packet, packet.conn);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private OperationContext setOperationContext(Operation op, String serviceName, Address caller, long callId, int partitionId) {
        OperationContext context = op.getOperationContext();
        context.setNodeService(this)
                .setService(getService(serviceName))
                .setCaller(caller)
                .setCallId(callId)
                .setPartitionId(partitionId);
        return context;
    }

    public Node getNode() {
        return node;
    }

    class Workers {
        final int threadCount;
        final ExecutorService[] workers;
        private final String threadNamePrefix;

        Workers(String s, int threadCount) {
            threadNamePrefix = s;
            this.threadCount = threadCount;
            workers = new ExecutorService[threadCount];
            for (int i = 0; i < threadCount; i++) {
                String threadName = threadNamePrefix + "_" + (i + 1);
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
                    new ThreadFactory() {
                        public Thread newThread(Runnable r) {
                            final Thread t = new Thread(node.threadGroup, r, node.getThreadNamePrefix(threadName), 0) {
                                public void run() {
                                    try {
                                        super.run();
                                    } finally {
                                        try {
                                            ThreadContext.shutdown(this);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            };
                            t.setContextClassLoader(node.getConfig().getClassLoader());
                            if (t.isDaemon()) {
                                t.setDaemon(false);
                            }
                            if (t.getPriority() != Thread.NORM_PRIORITY) {
                                t.setPriority(Thread.NORM_PRIORITY);
                            }
                            return t;
                        }
                    },
                    new RejectedExecutionHandler() {
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        }
                    }
            );
        }
    }

    public void registerService(String serviceName, Object obj) {
        services.put(serviceName, obj);
    }

    public Object getService(String serviceName) {
        return services.get(serviceName);
    }

    public ClusterImpl getClusterImpl() {
        return node.getClusterImpl();
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    public void notifyCall(long callId, Response response) {
        TheCall call = (TheCall) node.concurrentMapManager.removeRemoteCall(callId);
        if (call != null) {
            call.offerResponse(response);
        }
    }

    public PartitionInfo getPartitionInfo(int partitionId) {
        return node.concurrentMapManager.getPartitionInfo(partitionId);
    }

    private ExecutorService getExecutor(int partitionId, boolean nonBlocking) {
        if (partitionId > -1) {
            return (nonBlocking) ? nonBlockingWorkers.getExecutor(partitionId) : blockingWorkers.getExecutor(partitionId);
        } else {
            return executorService;
        }
    }

    public boolean isOwner(int partitionId) {
        return node.concurrentMapManager.getPartitionServiceImpl().getPartition(partitionId).getOwner().localMember();
    }

    public void checkTarget(int partitionId, int replicaIndex) throws WrongTargetException {
        PartitionInfo p = getPartitionInfo(partitionId);
        Address target = p.getReplicaAddress(replicaIndex);
        if (!getThisAddress().equals(target)) {
            throw new WrongTargetException(getThisAddress(), target);
        }
    }
}

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
    private final Workers workers2 = new Workers("hz.NonBlocking", 1);
    private final Workers workers = new Workers("hz.Blocking", 8);
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

    public Future invoke(final String serviceName, Operation op, Address target) throws Exception {
        if (target == null) {
            throw new NullPointerException(op + ": Target is null");
        }
        if (getThisAddress().equals(target)) {
            return invokeLocally(serviceName, op);
        } else {
            return invokeRemotely(serviceName, target, op);
        }
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception {
        Map<Member, ArrayList<Integer>> memberPartitions = getMemberPartitions();
        List<Future> responses = new ArrayList<Future>(memberPartitions.size());
        Data data = toData(op);
        for (Map.Entry<Member, ArrayList<Integer>> mp : memberPartitions.entrySet()) {
            Address target = ((MemberImpl) mp.getKey()).getAddress();
            responses.add(invoke(serviceName, new PartitionIterator(mp.getValue(), data), target));
        }
        Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(PARTITION_COUNT);
        for (Future response : responses) {
            Response r = (Response) response.get();
            Map<Integer, Object> partialResult = (Map<Integer, Object>) r.getResult();
            System.out.println(partialResult);
            partitionResults.putAll(partialResult);
        }
        return partitionResults;
    }

    public Member getOwner(int partitionId) {
        return node.concurrentMapManager.getPartitionServiceImpl().getPartition(partitionId).getOwner();
    }

    Map<Member, ArrayList<Integer>> getMemberPartitions() {
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

    private Future invokeLocally(final String serviceName, final Operation op) {
        final int partitionId = (op instanceof KeyBasedOperation)
                ? getPartitionId(((KeyBasedOperation) op).getKey())
                : -1;
        return invokeLocally(serviceName, partitionId, op);
    }

    private Future invokeLocally(final String serviceName, final int partitionId, final Operation op) {
        setOperationContext(op, serviceName, node.getThisAddress(), -1, partitionId)
                .setLocal(true);
        return runLocally(partitionId, op, (op instanceof BackupOperation));
    }

    private Future invokeRemotely(final String serviceName, Address target, Operation op) throws Exception {
        return invokeRemotely(serviceName, target, op, null);
    }

    private Future invokeRemotely(final String serviceName, Address target, Operation op, Callback callback) throws Exception {
        if (target == null) throw new NullPointerException("Target is null");
        if (node.getClusterImpl().getMember(target) == null) {
            throw new RuntimeException("TargetNotClusterMember[" + target + "]");
        }
        if (getThisAddress().equals(target)) {
            throw new RuntimeException("RemoteTarget cannot be local!");
        }
        final int partitionId = (op instanceof KeyBasedOperation)
                ? getPartitionId(((KeyBasedOperation) op).getKey())
                : -1;
        final Packet packet = new Packet();
        packet.operation = REMOTE_CALL;
        packet.blockId = partitionId;
        packet.name = serviceName;
        packet.longValue = (op instanceof BackupOperation) ? 1 : 0;
        Data valueData = toData(op);
        packet.setValue(valueData);
        TheCall call = new TheCall(target, op, callback);
        boolean sent = node.concurrentMapManager.registerAndSend(target, packet, call);
        if (!sent) {
            throw new IOException();
        }
        return call;
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
                    Object obj = toObject(data);
                    Operation op = (Operation) obj;
                    setOperationContext(op, serviceName, caller, callId, partitionId);
                    Object response = op.call();
                    if (!(op instanceof NoResponse)) {
                        packet.clearForResponse();
                        packet.blockId = partitionId;
                        packet.callId = callId;
                        packet.longValue = (response instanceof BackupOperation) ? 1 : 0;
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

    public ExecutorService getExecutor(int partitionId, boolean nonBlocking) {
        if (partitionId > -1) {
            return (nonBlocking) ? workers2.getExecutor(partitionId) : workers.getExecutor(partitionId);
        } else {
            return executorService;
        }
    }

    public Future runLocally(final int partitionId, final Callable callable, final boolean nonBlocking) {
        final ExecutorService executor = getExecutor(partitionId, nonBlocking);
        return executor.submit(callable);
    }

    public boolean isOwner(int partitionId) {
        return node.concurrentMapManager.getPartitionServiceImpl().getPartition(partitionId).getOwner().localMember();
    }
}

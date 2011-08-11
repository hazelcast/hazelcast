/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.RemotelyProcessable;
import com.hazelcast.core.*;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl.CollectionProxyReal;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.DistributedTimeoutException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.impl.BaseManager.getInstanceType;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ClientService implements ConnectionListener {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new ConcurrentHashMap<Connection, ClientEndpoint>();
    private final ClientOperationHandler[] clientOperationHandlers = new ClientOperationHandler[300];
    private final ClientOperationHandler unknownOperationHandler = new UnknownClientOperationHandler();
    private final ILogger logger;
    private final int THREAD_COUNT;
    final Worker[] workers;

    public ClientService(Node node) {
        this.node = node;
        this.logger = node.getLogger(this.getClass().getName());
        node.getClusterImpl().addMembershipListener(new ClientServiceMembershipListener());
        clientOperationHandlers[CONCURRENT_MAP_PUT.getValue()] = new MapPutHandler();
        clientOperationHandlers[CONCURRENT_MAP_PUT_AND_UNLOCK.getValue()] = new MapPutAndUnlockHandler();
        clientOperationHandlers[CONCURRENT_MAP_PUT_ALL.getValue()] = new MapPutAllHandler();
        clientOperationHandlers[CONCURRENT_MAP_PUT_MULTI.getValue()] = new MapPutMultiHandler();
        clientOperationHandlers[CONCURRENT_MAP_PUT_IF_ABSENT.getValue()] = new MapPutIfAbsentHandler();
        clientOperationHandlers[CONCURRENT_MAP_TRY_PUT.getValue()] = new MapTryPutHandler();
        clientOperationHandlers[CONCURRENT_MAP_GET.getValue()] = new MapGetHandler();
        clientOperationHandlers[CONCURRENT_MAP_GET_ALL.getValue()] = new MapGetAllHandler();
        clientOperationHandlers[CONCURRENT_MAP_REMOVE.getValue()] = new MapRemoveHandler();
        clientOperationHandlers[CONCURRENT_MAP_TRY_REMOVE.getValue()] = new MapTryRemoveHandler();
        clientOperationHandlers[CONCURRENT_MAP_REMOVE_IF_SAME.getValue()] = new MapRemoveIfSameHandler();
        clientOperationHandlers[CONCURRENT_MAP_REMOVE_MULTI.getValue()] = new MapRemoveMultiHandler();
        clientOperationHandlers[CONCURRENT_MAP_EVICT.getValue()] = new MapEvictHandler();
        clientOperationHandlers[CONCURRENT_MAP_FLUSH.getValue()] = new MapFlushHandler();
        clientOperationHandlers[CONCURRENT_MAP_REPLACE_IF_NOT_NULL.getValue()] = new MapReplaceIfNotNullHandler();
        clientOperationHandlers[CONCURRENT_MAP_REPLACE_IF_SAME.getValue()] = new MapReplaceIfSameHandler();
        clientOperationHandlers[CONCURRENT_MAP_SIZE.getValue()] = new MapSizeHandler();
        clientOperationHandlers[CONCURRENT_MAP_GET_MAP_ENTRY.getValue()] = new GetMapEntryHandler();
        clientOperationHandlers[CONCURRENT_MAP_TRY_LOCK_AND_GET.getValue()] = new MapTryLockAndGetHandler();
        clientOperationHandlers[CONCURRENT_MAP_LOCK.getValue()] = new MapLockHandler();
        clientOperationHandlers[CONCURRENT_MAP_UNLOCK.getValue()] = new MapUnlockHandler();
        clientOperationHandlers[CONCURRENT_MAP_LOCK_MAP.getValue()] = new MapLockMapHandler();
        clientOperationHandlers[CONCURRENT_MAP_UNLOCK_MAP.getValue()] = new MapUnlockMapHandler();
        clientOperationHandlers[CONCURRENT_MAP_CONTAINS_KEY.getValue()] = new MapContainsHandler();
        clientOperationHandlers[CONCURRENT_MAP_CONTAINS_VALUE.getValue()] = new MapContainsValueHandler();
        clientOperationHandlers[CONCURRENT_MAP_ADD_TO_LIST.getValue()] = new ListAddHandler();
        clientOperationHandlers[CONCURRENT_MAP_ADD_TO_SET.getValue()] = new SetAddHandler();
        clientOperationHandlers[CONCURRENT_MAP_REMOVE_ITEM.getValue()] = new MapItemRemoveHandler();
        clientOperationHandlers[CONCURRENT_MAP_ITERATE_KEYS.getValue()] = new MapIterateKeysHandler();
        clientOperationHandlers[CONCURRENT_MAP_ITERATE_ENTRIES.getValue()] = new MapIterateEntriesHandler();
        clientOperationHandlers[CONCURRENT_MAP_VALUE_COUNT.getValue()] = new MapValueCountHandler();
        clientOperationHandlers[TOPIC_PUBLISH.getValue()] = new TopicPublishHandler();
        clientOperationHandlers[BLOCKING_QUEUE_OFFER.getValue()] = new QueueOfferHandler();
        clientOperationHandlers[BLOCKING_QUEUE_POLL.getValue()] = new QueuePollHandler();
        clientOperationHandlers[BLOCKING_QUEUE_REMOVE.getValue()] = new QueueRemoveHandler();
        clientOperationHandlers[BLOCKING_QUEUE_PEEK.getValue()] = new QueuePeekHandler();
        clientOperationHandlers[BLOCKING_QUEUE_SIZE.getValue()] = new QueueSizeHandler();
        clientOperationHandlers[BLOCKING_QUEUE_REMAINING_CAPACITY.getValue()] = new QueueRemainingCapacityHandler();
        clientOperationHandlers[BLOCKING_QUEUE_ENTRIES.getValue()] = new QueueEntriesHandler();
        clientOperationHandlers[TRANSACTION_BEGIN.getValue()] = new TransactionBeginHandler();
        clientOperationHandlers[TRANSACTION_COMMIT.getValue()] = new TransactionCommitHandler();
        clientOperationHandlers[TRANSACTION_ROLLBACK.getValue()] = new TransactionRollbackHandler();
        clientOperationHandlers[ADD_LISTENER.getValue()] = new AddListenerHandler();
        clientOperationHandlers[REMOVE_LISTENER.getValue()] = new RemoveListenerHandler();
        clientOperationHandlers[REMOTELY_PROCESS.getValue()] = new RemotelyProcessHandler();
        clientOperationHandlers[DESTROY.getValue()] = new DestroyHandler();
        clientOperationHandlers[GET_ID.getValue()] = new GetIdHandler();
        clientOperationHandlers[ADD_INDEX.getValue()] = new AddIndexHandler();
        clientOperationHandlers[NEW_ID.getValue()] = new NewIdHandler();
        clientOperationHandlers[EXECUTE.getValue()] = new ExecutorServiceHandler();
        clientOperationHandlers[CANCEL_EXECUTION.getValue()] = new CancelExecutionHandler();
        clientOperationHandlers[GET_INSTANCES.getValue()] = new GetInstancesHandler();
        clientOperationHandlers[GET_MEMBERS.getValue()] = new GetMembersHandler();
        clientOperationHandlers[GET_CLUSTER_TIME.getValue()] = new GetClusterTimeHandler();
        clientOperationHandlers[CLIENT_AUTHENTICATE.getValue()] = new ClientAuthenticateHandler();
        clientOperationHandlers[CLIENT_ADD_INSTANCE_LISTENER.getValue()] = new ClientAddInstanceListenerHandler();
        clientOperationHandlers[CLIENT_GET_PARTITIONS.getValue()] = new GetPartitionsHandler();
        clientOperationHandlers[ATOMIC_NUMBER_ADD_AND_GET.getValue()] = new AtomicLongAddAndGetHandler();
        clientOperationHandlers[ATOMIC_NUMBER_COMPARE_AND_SET.getValue()] = new AtomicLongCompareAndSetHandler();
        clientOperationHandlers[ATOMIC_NUMBER_GET_AND_SET.getValue()] = new AtomicLongGetAndSetHandler();
        clientOperationHandlers[ATOMIC_NUMBER_GET_AND_ADD.getValue()] = new AtomicLongGetAndAddHandler();
        clientOperationHandlers[COUNT_DOWN_LATCH_AWAIT.getValue()] = new CountDownLatchAwaitHandler();
        clientOperationHandlers[COUNT_DOWN_LATCH_COUNT_DOWN.getValue()] = new CountDownLatchCountDownHandler();
        clientOperationHandlers[COUNT_DOWN_LATCH_GET_COUNT.getValue()] = new CountDownLatchGetCountHandler();
        clientOperationHandlers[COUNT_DOWN_LATCH_GET_OWNER.getValue()] = new CountDownLatchGetOwnerHandler();
        clientOperationHandlers[COUNT_DOWN_LATCH_SET_COUNT.getValue()] = new CountDownLatchSetCountHandler();
        clientOperationHandlers[SEMAPHORE_ATTACH_DETACH_PERMITS.getValue()] = new SemaphoreAttachDetachHandler();
        clientOperationHandlers[SEMAPHORE_CANCEL_ACQUIRE.getValue()] = new SemaphoreCancelAcquireHandler();
        clientOperationHandlers[SEMAPHORE_DRAIN_PERMITS.getValue()] = new SemaphoreDrainHandler();
        clientOperationHandlers[SEMAPHORE_GET_ATTACHED_PERMITS.getValue()] = new SemaphoreGetAttachedHandler();
        clientOperationHandlers[SEMAPHORE_GET_AVAILABLE_PERMITS.getValue()] = new SemaphoreGetAvailableHandler();
        clientOperationHandlers[SEMAPHORE_REDUCE_PERMITS.getValue()] = new SemaphoreReduceHandler();
        clientOperationHandlers[SEMAPHORE_RELEASE.getValue()] = new SemaphoreReleaseHandler();
        clientOperationHandlers[SEMAPHORE_TRY_ACQUIRE.getValue()] = new SemaphoreTryAcquireHandler();
        node.connectionManager.addConnectionListener(this);
        this.THREAD_COUNT = node.getGroupProperties().EXECUTOR_CLIENT_THREAD_COUNT.getInteger();
        workers = new Worker[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            workers[i] = new Worker();
        }
    }

    boolean firstCall = true;

    // always called by InThread
    public void handle(Packet packet) {
        if (firstCall) {
            String threadNamePrefix = node.getThreadPoolNamePrefix("client.service");
            for (int i = 0; i < THREAD_COUNT; i++) {
                Worker worker = workers[i];
                new Thread(node.threadGroup, worker, threadNamePrefix + i).start();
            }
            firstCall = false;
        }
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        ClientOperationHandler clientOperationHandler = clientOperationHandlers[packet.operation.getValue()];
        if (clientOperationHandler == null) {
            clientOperationHandler = unknownOperationHandler;
        }
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, packet, callContext, clientOperationHandler);
        clientEndpoint.addRequest(clientRequestHandler);
        if (packet.operation == CONCURRENT_MAP_UNLOCK) {
            node.executorManager.executeNow(clientRequestHandler);
        } else {
            int hash = hash(callContext.getThreadId(), THREAD_COUNT);
            workers[hash].addWork(clientRequestHandler);
        }
    }

    public void shutdown() {
        mapClientEndpoints.clear();
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    public void restart() {
        for (ListenerManager.ListenerItem listener : node.listenerManager.listeners) {
            if (listener instanceof ClientListener) {
                node.listenerManager.removeListener(listener.name, listener, listener.key);
            }
        }
    }

    private int hash(int id, int maxCount) {
        return (id == Integer.MIN_VALUE) ? 0 : Math.abs(id) % maxCount;
    }

    class Worker implements Runnable {
        private final BlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
        private volatile boolean active = true;

        public void run() {
            ThreadContext.get().setCurrentFactory(node.factory);
            while (active) {
                Runnable r = null;
                try {
                    r = q.take();
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    r.run();
                } catch (Throwable ignored) {
                }
            }
        }

        public void stop() {
            active = false;
            q.offer(new Runnable() {
                public void run() {
                }
            });
        }

        public void addWork(Runnable runnable) {
            q.offer(runnable);
        }
    }

    public int numberOfConnectedClients() {
        return mapClientEndpoints.size();
    }

    public ClientEndpoint getClientEndpoint(Connection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(node, conn);
            mapClientEndpoints.put(conn, clientEndpoint);
        }
        return clientEndpoint;
    }

    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(final Connection connection) {
        final ClientEndpoint clientEndpoint = removeClientEndpoint(connection);
        if (clientEndpoint != null) {
            node.executorManager.executeNow(new FallThroughRunnable() {
                public void doRun() {
                    clientEndpoint.connectionRemoved(connection);
                }
            });
        }
    }

    ClientEndpoint removeClientEndpoint(final Connection connection) {
        return mapClientEndpoints.remove(connection);
    }

    private class QueueOfferHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            long millis = (Long) toObject(value);
            try {
                if (millis == -1) {
                    queue.put(key);
                    return toData(true);
                } else if (millis == 0) {
                    return toData(queue.offer(key));
                } else {
                    return toData(queue.offer(key, (Long) toObject(value), TimeUnit.MILLISECONDS));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }
    }

    private class QueuePollHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            try {
                long millis = (Long) toObject(value);
                if (millis == -1) {
                    return (Data) queue.take();
                } else if (millis == 0) {
                    return (Data) queue.poll();
                } else {
                    return (Data) queue.poll((Long) millis, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class QueueRemoveHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            if (value != null) {
                return toData(queue.remove(value));
            } else {
                return (Data) queue.remove();
            }
        }
    }

    private class QueuePeekHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            return (Data) queue.peek();
        }
    }

    private class QueueSizeHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            return toData(queue.size());
        }
    }

    private class TopicPublishHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ITopic<Object> topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
            topic.publish(packet.getKeyData());
        }
    }

    private class QueueRemainingCapacityHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            return toData(queue.remainingCapacity());
        }
    }

    private class QueueEntriesHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            Object[] array = queue.toArray();
            return toData(array);
        }
    }

    private class RemotelyProcessHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            node.clusterService.enqueuePacket(packet);
        }

        @Override
        protected void sendResponse(Packet request) {
        }
    }

    private class DestroyHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Instance instance = (Instance) node.factory.getOrCreateProxyByName(packet.name);
            instance.destroy();
        }
    }

    private class NewIdHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            IdGenerator idGen = (IdGenerator) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(idGen.newId()));
        }
    }

    private class MapPutMultiHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(multiMap.put(packet.getKeyData(), packet.getValueData())));
        }
    }

    private class MapValueCountHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(multiMap.valueCount(packet.getKeyData())));
        }
    }

    private class MapRemoveMultiHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            if (packet.getValueData() == null || packet.getValueData().size() == 0) {
                FactoryImpl.MultiMapProxyImpl mmProxyImpl = (FactoryImpl.MultiMapProxyImpl) multiMap;
                FactoryImpl.MultiMapProxyImpl.MultiMapReal real = mmProxyImpl.getBase();
                MProxy mapProxy = real.mapProxy;
                packet.setValue((Data) mapProxy.remove(packet.getKeyData()));
            } else {
                packet.setValue(toData(multiMap.remove(packet.getKeyData(), packet.getValueData())));
            }
        }
    }

    private class CancelExecutionHandler extends ClientOperationHandler {

        @Override
        public void processCall(Node node, Packet packet) {
            long taskId = (Long) toObject(packet.getKeyData());
            boolean mayInterruptIfRunning = (Boolean) toObject(packet.getValue());
            ClientEndpoint thisEndPoint = getClientEndpoint(packet.conn);
            DistributedTask task = thisEndPoint.getTask(taskId);
            boolean cancelled = task.cancel(mayInterruptIfRunning);
            if (cancelled) {
                thisEndPoint.removeTask(taskId);
            }
            packet.setValue(toData(cancelled));
        }
    }

    private class ExecutorServiceHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
        }

        @Override
        public final void handle(Node node, final Packet packet) {
            try {
                String name = packet.name;
                ExecutorService executorService = node.factory.getExecutorService(name);
                ClientDistributedTask cdt = (ClientDistributedTask) toObject(packet.getKeyData());
                DistributedTask task;
                if (cdt.getKey() != null) {
                    task = new DistributedTask(cdt.getCallable(), cdt.getKey());
                } else if (cdt.getMember() != null) {
                    task = new DistributedTask(cdt.getCallable(), cdt.getMember());
                } else if (cdt.getMembers() != null) {
                    task = new MultiTask(cdt.getCallable(), cdt.getMembers());
                } else {
                    task = new DistributedTask(cdt.getCallable());
                }
                final ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
                clientEndpoint.storeTask(packet.callId, task);
                task.setExecutionCallback(new ExecutionCallback() {
                    public void done(Future future) {
                        Object result;
                        try {
                            clientEndpoint.removeTask(packet.callId);
                            result = future.get();
                            packet.setValue(toData(result));
                        } catch (InterruptedException e) {
                            return;
                        } catch (CancellationException e) {
                            packet.setValue(toData(e));
                        } catch (ExecutionException e) {
                            packet.setValue(toData(e));
                        }
                        sendResponse(packet);
                    }
                });
                executorService.execute(task);
            } catch (RuntimeException e) {
                logger.log(Level.WARNING,
                        "exception during handling " + packet.operation + ": " + e.getMessage(), e);
                packet.clearForResponse();
                packet.setValue(toData(e));
            }
        }
    }

    private class GetInstancesHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Collection<Instance> instances = node.factory.getInstances();
            ArrayList<Object> instanceIds = new ArrayList<Object>();
             for (Instance instance : instances) {
                Object id = instance.getId();
                if (id instanceof FactoryImpl.ProxyKey) {
                    Object key = ((FactoryImpl.ProxyKey) id).getKey();
                    if (key instanceof Instance)
                        id = key.toString();
                }
                String idStr = id.toString();
                if (!idStr.startsWith(Prefix.MAP_OF_LIST) && !idStr.startsWith(Prefix.MAP_FOR_QUEUE)) {
                    instanceIds.add(id);
                }
            }
            packet.setValue(toData(instanceIds.toArray()));
        }
    }

    private class GetMembersHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Cluster cluster = node.factory.getCluster();
            Set<Member> members = cluster.getMembers();
            Set<Data> setData = new LinkedHashSet<Data>();
            if (members != null) {
                for (Iterator<Member> iterator = members.iterator(); iterator.hasNext();) {
                    Member member = iterator.next();
                    setData.add(toData(member));
                }
                Keys keys = new Keys(setData);
                packet.setValue(toData(keys));
            }
        }
    }

    private class GetPartitionsHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            PartitionService partitionService = node.factory.getPartitionService();
            if (packet.getKeyData() != null && packet.getKeyData().size() > 0) {
                Object key = toObject(packet.getKeyData());
                Partition partition = partitionService.getPartition(key);
                Data value = toData(new PartitionImpl(partition.getPartitionId(), (MemberImpl) partition.getOwner()));
                packet.setValue(value);
            } else {
                Set<Partition> partitions = partitionService.getPartitions();
                Set<Data> setData = new LinkedHashSet<Data>();
                for (Iterator<Partition> iterator = partitions.iterator(); iterator.hasNext();) {
                    Partition partition = iterator.next();
                    setData.add(toData(new PartitionImpl(partition.getPartitionId(), (MemberImpl) partition.getOwner())));
                }
                Keys keys = new Keys(setData);
                packet.setValue(toData(keys));
            }
        }
    }

    abstract private class AtomicLongClientHandler extends ClientOperationHandler {
        abstract Object processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected);

        public void processCall(Node node, Packet packet) {
            final AtomicNumberProxy atomicLong = (AtomicNumberProxy) node.factory.getAtomicNumber(packet.name);
            final Long value = (Long) toObject(packet.getValueData());
            final Long expectedValue = (Long) toObject(packet.getKeyData());
            packet.setValue(toData(processCall(atomicLong, value, expectedValue)));
        }
    }

    private class AtomicLongAddAndGetHandler extends AtomicLongClientHandler {
        public Long processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
            return atomicLongProxy.addAndGet(value);
        }
    }

    private class AtomicLongCompareAndSetHandler extends AtomicLongClientHandler {
        public Boolean processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
            return atomicLongProxy.compareAndSet(expected, value);
        }
    }

    private class AtomicLongGetAndAddHandler extends AtomicLongClientHandler {
        public Long processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
            return atomicLongProxy.getAndAdd(value);
        }
    }

    private class AtomicLongGetAndSetHandler extends AtomicLongClientHandler {
        public Long processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
            return atomicLongProxy.getAndSet(value);
        }
    }

    abstract private class CountDownLatchClientHandler extends ClientOperationHandler {
        abstract void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value);

        public void processCall(Node node, Packet packet) {
            final String name = packet.name.substring(Prefix.COUNT_DOWN_LATCH.length());
            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) node.factory.getCountDownLatch(name);
            final Integer value = (Integer) toObject(packet.getValueData());
            processCall(packet, cdlProxy, value);
        }
    }

    private class CountDownLatchAwaitHandler extends CountDownLatchClientHandler {
        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
            try {
                packet.setValue(toData(cdlProxy.await(packet.timeout, TimeUnit.MILLISECONDS)));
            } catch (Throwable e) {
                packet.setValue(toData(new ClientServiceException(e)));
            }
        }
    }

    private class CountDownLatchCountDownHandler extends CountDownLatchClientHandler {
        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
            cdlProxy.countDown();
        }
    }

    private class CountDownLatchGetCountHandler extends CountDownLatchClientHandler {
        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
            packet.setValue(toData(cdlProxy.getCount()));
        }
    }

    private class CountDownLatchGetOwnerHandler extends CountDownLatchClientHandler {
        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
            packet.setValue(toData(cdlProxy.getOwnerAddress()));
        }
    }

    private class CountDownLatchSetCountHandler extends CountDownLatchClientHandler {
        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer count) {
            Address ownerAddress = packet.conn.getEndPoint();
            packet.setValue(toData(cdlProxy.setCount(count, ownerAddress)));
        }
    }


    public static class CountDownLatchLeave implements RemotelyProcessable {
        Address deadAddress;
        transient Node node;

        public CountDownLatchLeave(Address deadAddress) {
            this.deadAddress = deadAddress;
        }

        public CountDownLatchLeave() {
        }

        public void setConnection(Connection conn) {
        }

        public void writeData(DataOutput out) throws IOException {
            deadAddress.writeData(out);
        }

        public void readData(DataInput in) throws IOException {
            (deadAddress = new Address()).readData(in);
        }

        public Node getNode() {
            return node;
        }

        public void setNode(Node node) {
            this.node = node;
        }

        public void process() {
            node.concurrentMapManager.syncForDeadCountDownLatches(deadAddress);
        }
    }

    abstract private class SemaphoreClientOperationHandler extends ClientOperationHandler {
        abstract void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag);

        public void processCall(Node node, Packet packet) {
            final SemaphoreProxy semaphoreProxy = (SemaphoreProxy) node.factory.getSemaphore(packet.name);
            final Integer value = (Integer) toObject(packet.getValueData());
            final boolean flag = (Boolean) toObject(packet.getKeyData());
            processCall(packet, semaphoreProxy, value, flag);
        }
    }

    private class SemaphoreAttachDetachHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean attach){
            ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
            if (attach) {
                semaphoreProxy.attach(permits);
                clientEndpoint.attachDetachPermits(semaphoreProxy.getName(), permits);
            } else {
                semaphoreProxy.detach(permits);
                clientEndpoint.attachDetachPermits(semaphoreProxy.getName(), -permits);
            }
        }
    }

    private class SemaphoreCancelAcquireHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ConcurrentMapManager.MSemaphore msemaphore = node.concurrentMapManager.new MSemaphore();
            packet.setValue(toData(msemaphore.cancelAcquire(toData(packet.name))));
        }
    }

    private class SemaphoreGetAttachedHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag){
            packet.setValue(toData(semaphoreProxy.attachedPermits()));
        }
    }

    private class SemaphoreGetAvailableHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag){
            packet.setValue(toData(semaphoreProxy.availablePermits()));
        }
    }

    private class SemaphoreDrainHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag){
            packet.setValue(toData(semaphoreProxy.drainPermits()));
        }
    }

    private class SemaphoreReduceHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean flag){
            semaphoreProxy.reducePermits(permits);
        }
    }

    private class SemaphoreReleaseHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean detach){
            if (detach) {
                semaphoreProxy.releaseDetach(permits);
                getClientEndpoint(packet.conn).attachDetachPermits(packet.name, -permits);
            } else {
                semaphoreProxy.release(permits);
            }
        }
    }

    private class SemaphoreTryAcquireHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean attach){
            try {
                boolean acquired;
                if (attach) {
                    acquired = semaphoreProxy.tryAcquireAttach(permits, packet.timeout, TimeUnit.MILLISECONDS);
                    if(acquired){
                        getClientEndpoint(packet.conn).attachDetachPermits(packet.name, permits);
                    }
                } else {
                    acquired = semaphoreProxy.tryAcquire(permits, packet.timeout, TimeUnit.MILLISECONDS);
                }
                packet.setValue(toData(acquired));
            } catch (Throwable e) {
                packet.setValue(toData(new ClientServiceException(e)));
            }
        }
    }

    public static class PartitionImpl implements Partition, HazelcastInstanceAware, DataSerializable, Comparable {
        int partitionId;
        MemberImpl owner;

        PartitionImpl(int partitionId, MemberImpl owner) {
            this.partitionId = partitionId;
            this.owner = owner;
        }

        public PartitionImpl() {
        }

        public int getPartitionId() {
            return partitionId;
        }

        public Member getOwner() {
            return owner;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            if (owner != null) {
                owner.setHazelcastInstance(hazelcastInstance);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(partitionId);
            boolean hasOwner = (owner != null);
            out.writeBoolean(hasOwner);
            if (hasOwner) {
                owner.writeData(out);
            }
        }

        public void readData(DataInput in) throws IOException {
            partitionId = in.readInt();
            boolean hasOwner = in.readBoolean();
            if (hasOwner) {
                owner = new MemberImpl();
                owner.readData(in);
            }
        }

        public int compareTo(Object o) {
            PartitionImpl partition = (PartitionImpl) o;
            Integer id = partitionId;
            return (id.compareTo(partition.getPartitionId()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionImpl partition = (PartitionImpl) o;
            return partitionId == partition.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "Partition [" +
                    +partitionId +
                    "], owner=" + owner;
        }
    }

    private class GetClusterTimeHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Cluster cluster = node.factory.getCluster();
            long clusterTime = cluster.getClusterTime();
            packet.setValue(toData(clusterTime));
        }
    }

    class ClientAuthenticateHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            String nodeGroupName = node.factory.getConfig().getGroupConfig().getName();
            String nodeGroupPassword = node.factory.getConfig().getGroupConfig().getPassword();
            Object groupName = toObject(packet.getKeyData());
            Object groupPassword = toObject(packet.getValueData());
            boolean authenticated = (nodeGroupName.equals(groupName) && nodeGroupPassword.equals(groupPassword));
            logger.log((authenticated ? Level.INFO : Level.WARNING), "received auth from " + packet.conn
                    + ", this group name:" + nodeGroupName + ", auth group name:" + groupName
                    + ", " + (authenticated ?
                    "successfully authenticated" : "authentication failed"));
            packet.clearForResponse();
            packet.setValue(toData(authenticated));
        }

        @Override
        public void postHandle(Packet packet) {
            boolean authenticated = (Boolean) toObject(packet.getValueData());
            if (!authenticated) {
                node.clientService.removeClientEndpoint(packet.conn);
            }
        }
    }

    private class ClientAddInstanceListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ClientEndpoint endPoint = getClientEndpoint(packet.conn);
            node.factory.addInstanceListener(endPoint);
        }
    }

    private class GetIdHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.getId());
        }
    }

    private class AddIndexHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.addIndex((String) toObject(key), (Boolean) toObject(value));
            return null;
        }
    }

    private class MapPutAllHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> imap, Data key, Data value) {
            MProxy mproxy = (MProxy) imap;
            Pairs pairs = (Pairs) toObject(value);
            try {
                node.concurrentMapManager.doPutAll(mproxy.getLongName(), pairs);
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
            return null;
        }
    }

    private class MapPutHandler extends ClientMapOperationHandlerWithTTL {

        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            if (ttl <= 0) {
                return (Data) map.put(key, v);
            } else {
                return (Data) map.put(key, v, ttl, TimeUnit.MILLISECONDS);
            }
        }
    }

    private class MapTryPutHandler extends ClientMapOperationHandlerWithTTL {
        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            return toData(map.tryPut(key, v, ttl, TimeUnit.MILLISECONDS));
        }
    }

    private class MapPutAndUnlockHandler extends ClientMapOperationHandlerWithTTL {
        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            map.putAndUnlock(key, v);
            return null;
        }
    }

    private class MapTryRemoveHandler extends ClientMapOperationHandlerWithTTL {
        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            try {
                return toData(map.tryRemove(key, ttl, TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                return toData(new DistributedTimeoutException());
            }
        }
    }

    private class MapTryLockAndGetHandler extends ClientMapOperationHandlerWithTTL {
        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            try {
                return toData(map.tryLockAndGet(key, ttl, TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                return toData(new DistributedTimeoutException());
            }
        }
    }

    private class MapPutIfAbsentHandler extends ClientOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            if (ttl <= 0) {
                return (Data) map.putIfAbsent(key, v);
            } else {
                return (Data) map.putIfAbsent(key, v, ttl, TimeUnit.MILLISECONDS);
            }
        }

        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            long ttl = packet.timeout;
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData(), ttl);
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    private class MapGetAllHandler extends ClientOperationHandler {

        public void processCall(Node node, Packet packet) {
            Keys keys = (Keys) toObject(packet.getKeyData());
            Pairs pairs = node.concurrentMapManager.getAllPairs(packet.name, keys);
            packet.clearForResponse();
            packet.setValue(toData(pairs));
        }
    }

    private class GetMapEntryHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.getMapEntry(key));
        }
    }

    private class MapGetHandler extends ClientOperationHandler {

        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            Data key = packet.getKeyData();
            if (instanceType == InstanceType.MAP) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.setKey(null);
                packet.setValue((Data) map.get(key));
            } else if (instanceType == InstanceType.MULTIMAP) {
                FactoryImpl.MultiMapProxyImpl multiMapImpl = (FactoryImpl.MultiMapProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
                FactoryImpl.MultiMapProxyImpl.MultiMapReal real = multiMapImpl.getBase();
                MProxy mapProxy = real.mapProxy;
                packet.setKey(null);
                packet.setValue((Data) mapProxy.get(key));
            }
        }
    }

    private class MapRemoveHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return (Data) map.remove(key);
        }
    }

    private class MapRemoveIfSameHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.remove(key, value));
        }
    }

    private class MapEvictHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.evict(key));
        }
    }

    private class MapFlushHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.flush();
            return null;
        }
    }

    private class MapReplaceIfNotNullHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return (Data) map.replace(key, value);
        }
    }

    private class MapReplaceIfSameHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            Object[] arr = (Object[]) toObject(value);
            return toData(map.replace(key, arr[0], arr[1]));
        }
    }

    private class MapContainsHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(map.containsKey(packet.getKeyData())));
            } else if (instanceType.equals(InstanceType.LIST) || instanceType.equals(InstanceType.SET)) {
                Collection<Object> collection = (Collection) node.factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(collection.contains(packet.getKeyData())));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(multiMap.containsKey(packet.getKeyData())));
            }
        }
    }

    private class MapContainsValueHandler extends ClientOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.containsValue(value));
        }

        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(map.containsValue(packet.getValueData())));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
                if (packet.getKeyData() != null && packet.getKeyData().size() > 0) {
                    packet.setValue(toData(multiMap.containsEntry(packet.getKeyData(), packet.getValueData())));
                } else {
                    packet.setValue(toData(multiMap.containsValue(packet.getValueData())));
                }
            }
        }
    }

    private class MapSizeHandler extends ClientCollectionOperationHandler {
        @Override
        public void doListOp(Node node, Packet packet) {
            IList<Object> list = (IList) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(list.size()));
        }

        @Override
        public void doMapOp(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(map.size()));
        }

        @Override
        public void doSetOp(Node node, Packet packet) {
            ISet<Object> set = (ISet) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(set.size()));
        }

        @Override
        public void doMultiMapOp(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(multiMap.size()));
        }

        @Override
        public void doQueueOp(Node node, Packet packet) {
            //ignore
        }
    }

    private class MapLockHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            throw new RuntimeException("Shouldn't invoke this method");
        }

        @Override
        public void processCall(Node node, Packet packet) {
            Instance.InstanceType type = getInstanceType(packet.name);
            long timeout = packet.timeout;
            Data value = null;
            IMap<Object, Object> map = null;
            if (type == Instance.InstanceType.MAP) {
                map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            } else {
                MultiMapProxy multiMapProxy = (MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
                map = multiMapProxy.getMProxy();
            }
            if (timeout == -1) {
                map.lock(packet.getKeyData());
                value = null;
            } else if (timeout == 0) {
                value = toData(map.tryLock(packet.getKeyData()));
            } else {
                value = toData(map.tryLock(packet.getKeyData(), timeout, (TimeUnit) toObject(packet.getValueData())));
            }
            packet.setValue(value);
        }
    }

    private class MapUnlockHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.unlock(key);
            return null;
        }

        @Override
        public void processCall(Node node, Packet packet) {
            Instance.InstanceType type = getInstanceType(packet.name);
            IMap<Object, Object> map = null;
            if (type == Instance.InstanceType.MAP) {
                map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            } else {
                MultiMapProxy multiMapProxy = (MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
                map = multiMapProxy.getMProxy();
            }
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    private class MapLockMapHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            throw new RuntimeException("Shouldn't invoke this method");
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            long timeout = packet.timeout;
            Data value = toData(map.lockMap(timeout, (TimeUnit) toObject(packet.getValueData())));
            packet.setValue(value);
        }
    }

    private class MapUnlockMapHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.unlockMap();
            return null;
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    private class TransactionBeginHandler extends ClientTransactionOperationHandler {
        public void processTransactionOp(Transaction transaction) {
            transaction.begin();
        }
    }

    private class TransactionCommitHandler extends ClientTransactionOperationHandler {
        public void processTransactionOp(Transaction transaction) {
            transaction.commit();
        }
    }

    private class TransactionRollbackHandler extends ClientTransactionOperationHandler {
        public void processTransactionOp(Transaction transaction) {
            transaction.rollback();
        }
    }

    private class MapIterateEntriesHandler extends ClientCollectionOperationHandler {
        public Data getMapKeys(final IMap<Object, Object> map, final Data key, final Data value) {
            Entries entries = null;
            if (value == null) {
                entries = (Entries) map.entrySet();
            } else {
                final Predicate p = (Predicate) toObject(value);
                entries = (Entries) map.entrySet(p);
            }
            final List<Map.Entry> list = entries.getKeyValues();
            final Keys keys = new Keys(new ArrayList<Data>(list.size() << 1));
            for (final Object obj : list) {
                final KeyValue entry = (KeyValue) obj;
                keys.add(toData(entry));
            }
            return toData(keys);
        }

        @Override
        public void doListOp(final Node node, final Packet packet) {
            IMap mapProxy = (IMap) node.factory.getOrCreateProxyByName(Prefix.MAP + Prefix.QUEUE + packet.name);
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData()));
        }

        @Override
        public void doMapOp(final Node node, final Packet packet) {
            packet.setValue(getMapKeys((IMap) node.factory.getOrCreateProxyByName(packet.name), packet.getKeyData(), packet.getValueData()));
        }

        @Override
        public void doSetOp(final Node node, final Packet packet) {
            final CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            final MProxy mapProxy = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData()));
        }

        @Override
        public void doMultiMapOp(final Node node, final Packet packet) {
            final FactoryImpl.MultiMapProxyImpl multiMapImpl = (FactoryImpl.MultiMapProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            final FactoryImpl.MultiMapProxyImpl.MultiMapReal real = multiMapImpl.getBase();
            final MProxy mapProxy = real.mapProxy;
            final Data value = getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }

        @Override
        public void doQueueOp(final Node node, final Packet packet) {
            final IQueue queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
        }
    }

    private class MapIterateKeysHandler extends ClientCollectionOperationHandler {
        public Data getMapKeys(IMap<Object, Object> map, Data key, Data value, Collection<Data> collection) {
            Entries entries = null;
            if (value == null) {
                entries = (Entries) map.keySet();
            } else {
                Predicate p = (Predicate) toObject(value);
                entries = (Entries) map.keySet(p);
            }
            List<Map.Entry> list = entries.getKeyValues();
            Keys keys = new Keys(collection);
            for (Object obj : list) {
                KeyValue entry = (KeyValue) obj;
                keys.add(entry.getKeyData());
            }
            return toData(keys);
        }

        @Override
        public void doListOp(Node node, Packet packet) {
            IMap mapProxy = (IMap) node.factory.getOrCreateProxyByName(Prefix.MAP + Prefix.QUEUE + packet.name);
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new ArrayList<Data>()));
        }

        @Override
        public void doMapOp(Node node, Packet packet) {
            packet.setValue(getMapKeys((IMap) node.factory.getOrCreateProxyByName(packet.name), packet.getKeyData(), packet.getValueData(), new HashSet<Data>()));
        }

        @Override
        public void doSetOp(Node node, Packet packet) {
            CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            MProxy mapProxy = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new HashSet<Data>()));
        }

        public void doMultiMapOp(Node node, Packet packet) {
            FactoryImpl.MultiMapProxyImpl multiMapImpl = (FactoryImpl.MultiMapProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            FactoryImpl.MultiMapProxyImpl.MultiMapReal real = multiMapImpl.getBase();
            MProxy mapProxy = real.mapProxy;
            Data value = getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new HashSet<Data>());
            packet.clearForResponse();
            packet.setValue(value);
        }

        public void doQueueOp(Node node, Packet packet) {
            IQueue queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
        }
    }

    private class AddListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, final Packet packet) {
            final ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
            boolean includeValue = (int) packet.longValue == 1;
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.addThisAsListener(map, packet.getKeyData(), includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.LIST)) {
                ListProxyImpl listProxy = (ListProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
                IMap map = (IMap) node.factory.getOrCreateProxyByName(Prefix.MAP + (String) listProxy.getId());
                clientEndpoint.addThisAsListener(map, null, true);
            } else if (getInstanceType(packet.name).equals(InstanceType.SET)) {
                CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
                IMap map = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
                clientEndpoint.addThisAsListener(map, null, includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.QUEUE)) {
                IQueue<Object> queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
                final String packetName = packet.name;
                ItemListener itemListener = new ClientItemListener(clientEndpoint, packetName);
                queue.addItemListener(itemListener, includeValue);
                clientEndpoint.queueItemListeners.put(queue, itemListener);
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic<Object> topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
                final String packetName = packet.name;
                MessageListener<Object> messageListener = new ClientMessageListener<Object>(clientEndpoint, packetName);
                topic.addMessageListener(messageListener);
                clientEndpoint.messageListeners.put(topic, messageListener);
            }
            packet.clearForResponse();
        }
    }

    public interface ClientListener {

    }

    public class ClientItemListener implements ItemListener, ClientListener {
        final ClientEndpoint clientEndpoint;
        final String name;

        ClientItemListener(ClientEndpoint clientEndpoint, String name) {
            this.clientEndpoint = clientEndpoint;
            this.name = name;
        }

        public void itemAdded(Object item) {
            Packet p = new Packet();
            p.set(name, ClusterOperation.EVENT, item, true);
            clientEndpoint.sendPacket(p);
        }

        public void itemRemoved(Object item) {
            Packet p = new Packet();
            p.set(name, ClusterOperation.EVENT, item, false);
            clientEndpoint.sendPacket(p);
        }
    }

    public class ClientMessageListener<T> implements MessageListener<T>, ClientListener {
        final ClientEndpoint clientEndpoint;
        final String name;

        ClientMessageListener(ClientEndpoint clientEndpoint, String name) {
            this.clientEndpoint = clientEndpoint;
            this.name = name;
        }

        public void onMessage(T msg) {
            Packet p = new Packet();
            p.set(name, ClusterOperation.EVENT, msg, null);
            clientEndpoint.sendPacket(p);
        }
    }

    private class RemoveListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.removeThisListener(map, packet.getKeyData());
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
                topic.removeMessageListener(clientEndpoint.messageListeners.remove(topic));
            } else if (getInstanceType(packet.name).equals(InstanceType.QUEUE)) {
                IQueue queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
                queue.removeItemListener(clientEndpoint.queueItemListeners.remove(queue));
            }
        }
    }

    private class ListAddHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
            IList list = (IList) node.factory.getOrCreateProxyByName(packet.name);
            Boolean value = list.add(packet.getKeyData());
            packet.clearForResponse();
            packet.setValue(toData(value));
        }
    }

    private class SetAddHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
            ISet list = (ISet) node.factory.getOrCreateProxyByName(packet.name);
            Boolean value = list.add(packet.getKeyData());
            packet.clearForResponse();
            packet.setValue(toData(value));
        }
    }

    private class MapItemRemoveHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Collection collection = (Collection) node.factory.getOrCreateProxyByName(packet.name);
            Data value = toData(collection.remove(packet.getKeyData()));
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    public abstract class ClientOperationHandler {
        public abstract void processCall(Node node, Packet packet);

        public void handle(Node node, Packet packet) {
            try {
                processCall(node, packet);
            } catch (RuntimeException e) {
                logger.log(Level.WARNING,
                        "exception during handling " + packet.operation + ": " + e.getMessage(), e);
                packet.clearForResponse();
                packet.setValue(toData(new ClientServiceException(e)));
            }
            sendResponse(packet);
        }

        protected void sendResponse(Packet request) {
            request.lockAddress = null;
            request.operation = RESPONSE;
            request.responseType = RESPONSE_SUCCESS;
            if (request.conn != null && request.conn.live()) {
                request.conn.getWriteHandler().enqueueSocketWritable(request);
            } else {
                logger.log(Level.WARNING, "unable to send response " + request);
            }
        }

        public void postHandle(Packet packet) {
        }
    }

    private final class UnknownClientOperationHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            final String error = "Unknown Client Operation, can not handle " + packet.operation;
            if (node.isActive()) {
                throw new RuntimeException(error);
            } else {
                logger.log(Level.WARNING, error);
            }
        }
    }

    private abstract class ClientMapOperationHandler extends ClientOperationHandler {
        public abstract Data processMapOp(IMap<Object, Object> map, Data key, Data value);

        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    private abstract class ClientMapOperationHandlerWithTTL extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            long ttl = packet.timeout;
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData(), ttl);
            packet.clearForResponse();
            packet.setValue(value);
        }

        protected abstract Data processMapOp(IMap<Object, Object> map, Data keyData, Data valueData, long ttl);
    }

    private abstract class ClientQueueOperationHandler extends ClientOperationHandler {
        public abstract Data processQueueOp(IQueue<Object> queue, Data key, Data value);

        public void processCall(Node node, Packet packet) {
            IQueue<Object> queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
            Data value = processQueueOp(queue, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    private abstract class ClientCollectionOperationHandler extends ClientOperationHandler {
        public abstract void doMapOp(Node node, Packet packet);

        public abstract void doListOp(Node node, Packet packet);

        public abstract void doSetOp(Node node, Packet packet);

        public abstract void doMultiMapOp(Node node, Packet packet);

        public abstract void doQueueOp(Node node, Packet packet);

        @Override
        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.LIST)) {
                doListOp(node, packet);
            } else if (instanceType.equals(InstanceType.SET)) {
                doSetOp(node, packet);
            } else if (instanceType.equals(InstanceType.MAP)) {
                doMapOp(node, packet);
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                doMultiMapOp(node, packet);
            } else if (instanceType.equals(InstanceType.QUEUE)) {
                doQueueOp(node, packet);
            }
        }
    }

    private abstract class ClientTransactionOperationHandler extends ClientOperationHandler {
        public abstract void processTransactionOp(Transaction transaction);

        public void processCall(Node node, Packet packet) {
            Transaction transaction = node.factory.getTransaction();
            processTransactionOp(transaction);
        }
    }

    private class ClientServiceMembershipListener implements MembershipListener {
        public void memberAdded(MembershipEvent membershipEvent) {
            notifyEndPoints(membershipEvent);
        }

        public void memberRemoved(MembershipEvent membershipEvent) {
            notifyEndPoints(membershipEvent);
        }

        void notifyEndPoints(MembershipEvent membershipEvent) {
            for (ClientEndpoint endpoint : mapClientEndpoints.values()) {
                Packet membershipEventPacket = endpoint.createMembershipEventPacket(membershipEvent);
                endpoint.sendPacket(membershipEventPacket);
            }
        }
    }
}

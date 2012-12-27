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

//package com.hazelcast.impl;
//
//import com.hazelcast.cluster.Bind;
//import com.hazelcast.cluster.RemotelyProcessable;
//import com.hazelcast.core.*;
//import com.hazelcast.core.Instance.InstanceType;
//import com.hazelcast.impl.base.KeyValue;
//import com.hazelcast.impl.base.Pairs;
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.nio.*;
//import com.hazelcast.partition.Partition;
//import com.hazelcast.partition.PartitionService;
//import com.hazelcast.query.Predicate;
//import com.hazelcast.security.Credentials;
//import com.hazelcast.security.UsernamePasswordCredentials;
//import com.hazelcast.util.DistributedTimeoutException;
//
//import javax.security.auth.login.LoginContext;
//import javax.security.auth.login.LoginException;
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.net.Socket;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.logging.Level;
//
//import static com.hazelcast.impl.BaseManager.getInstanceType;
//import static com.hazelcast.impl.ClusterOperation.*;
//import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
//import static com.hazelcast.nio.IOUtil.toData;
//import static com.hazelcast.nio.IOUtil.toObject;
//
//public class ClientHandlerService implements ConnectionListener {
//    private final Node node;
//    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new ConcurrentHashMap<Connection, ClientEndpoint>();
//    private final ClientCommandHandler[] clientOperationHandlers = new ClientCommandHandler[ClusterOperation.LENGTH];
//    private final ClientCommandHandler unknownOperationHandler = new UnknownClientOperationHandler();
//    private final ILogger logger;
//    private final int THREAD_COUNT;
//    final Worker[] workers;
//    private final FactoryImpl factory;
//    boolean firstCall = true;
//
//    public ClientHandlerService(Node node) {
//        this.node = node;
//        this.logger = node.getLogger(this.getClass().getName());
//        node.getClusterImpl().addMembershipListener(new ClientServiceMembershipListener());
//        registerHandler(CONCURRENT_MAP_PUT.getValue(), new MapPutHandler());
//        registerHandler(CONCURRENT_MAP_PUT.getValue(), new MapPutHandler());
//        registerHandler(CONCURRENT_MAP_PUT_AND_UNLOCK.getValue(), new MapPutAndUnlockHandler());
//        registerHandler(CONCURRENT_MAP_PUT_ALL.getValue(), new MapPutAllHandler());
//        registerHandler(CONCURRENT_MAP_PUT_MULTI.getValue(), new MapPutMultiHandler());
//        registerHandler(CONCURRENT_MAP_PUT_IF_ABSENT.getValue(), new MapPutIfAbsentHandler());
//        registerHandler(CONCURRENT_MAP_PUT_TRANSIENT.getValue(), new MapPutTransientHandler());
//        registerHandler(CONCURRENT_MAP_SET.getValue(), new MapSetHandler());
//        registerHandler(CONCURRENT_MAP_TRY_PUT.getValue(), new MapTryPutHandler());
//        registerHandler(CONCURRENT_MAP_GET.getValue(), new MapGetHandler());
//        registerHandler(CONCURRENT_MAP_GET_ALL.getValue(), new MapGetAllHandler());
//        registerHandler(CONCURRENT_MAP_REMOVE.getValue(), new MapRemoveHandler());
//        registerHandler(CONCURRENT_MAP_TRY_REMOVE.getValue(), new MapTryRemoveHandler());
//        registerHandler(CONCURRENT_MAP_REMOVE_IF_SAME.getValue(), new MapRemoveIfSameHandler());
//        registerHandler(CONCURRENT_MAP_REMOVE_MULTI.getValue(), new MapRemoveMultiHandler());
//        registerHandler(CONCURRENT_MAP_EVICT.getValue(), new MapEvictHandler());
//        registerHandler(CONCURRENT_MAP_FLUSH.getValue(), new MapFlushHandler());
//        registerHandler(CONCURRENT_MAP_REPLACE_IF_NOT_NULL.getValue(), new MapReplaceIfNotNullHandler());
//        registerHandler(CONCURRENT_MAP_REPLACE_IF_SAME.getValue(), new MapReplaceIfSameHandler());
//        registerHandler(CONCURRENT_MAP_SIZE.getValue(), new MapSizeHandler());
//        registerHandler(CONCURRENT_MAP_GET_MAP_ENTRY.getValue(), new GetMapEntryHandler());
//        registerHandler(CONCURRENT_MAP_TRY_LOCK_AND_GET.getValue(), new MapTryLockAndGetHandler());
//        registerHandler(CONCURRENT_MAP_LOCK.getValue(), new MapLockHandler());
//        registerHandler(CONCURRENT_MAP_IS_KEY_LOCKED.getValue(), new MapIsKeyLockedHandler());
//        registerHandler(CONCURRENT_MAP_UNLOCK.getValue(), new MapUnlockHandler());
//        registerHandler(CONCURRENT_MAP_FORCE_UNLOCK.getValue(), new MapForceUnlockHandler());
//        registerHandler(CONCURRENT_MAP_LOCK_MAP.getValue(), new MapLockMapHandler());
//        registerHandler(CONCURRENT_MAP_UNLOCK_MAP.getValue(), new MapUnlockMapHandler());
//        registerHandler(CONCURRENT_MAP_CONTAINS_KEY.getValue(), new MapContainsHandler());
//        registerHandler(CONCURRENT_MAP_CONTAINS_VALUE.getValue(), new MapContainsValueHandler());
//        registerHandler(CONCURRENT_MAP_ADD_TO_LIST.getValue(), new ListAddHandler());
//        registerHandler(CONCURRENT_MAP_ADD_TO_SET.getValue(), new SetAddHandler());
//        registerHandler(CONCURRENT_MAP_REMOVE_ITEM.getValue(), new MapItemRemoveHandler());
//        registerHandler(CONCURRENT_MAP_ITERATE_KEYS.getValue(), new MapIterateKeysHandler());
//        registerHandler(CONCURRENT_MAP_ITERATE_ENTRIES.getValue(), new MapIterateEntriesHandler());
//        registerHandler(CONCURRENT_MAP_VALUE_COUNT.getValue(), new MapValueCountHandler());
//        registerHandler(TOPIC_PUBLISH.getValue(), new TopicPublishHandler());
//        registerHandler(BLOCKING_QUEUE_OFFER.getValue(), new QueueOfferHandler());
//        registerHandler(BLOCKING_QUEUE_POLL.getValue(), new QueuePollHandler());
//        registerHandler(BLOCKING_QUEUE_REMOVE.getValue(), new QueueRemoveHandler());
//        registerHandler(BLOCKING_QUEUE_PEEK.getValue(), new QueuePeekHandler());
//        registerHandler(BLOCKING_QUEUE_SIZE.getValue(), new QueueSizeHandler());
//        registerHandler(BLOCKING_QUEUE_REMAINING_CAPACITY.getValue(), new QueueRemainingCapacityHandler());
//        registerHandler(BLOCKING_QUEUE_ENTRIES.getValue(), new QueueEntriesHandler());
//        registerHandler(TRANSACTION_BEGIN.getValue(), new TransactionBeginHandler());
//        registerHandler(TRANSACTION_COMMIT.getValue(), new TransactionCommitHandler());
//        registerHandler(TRANSACTION_ROLLBACK.getValue(), new TransactionRollbackHandler());
//        registerHandler(ADD_LISTENER.getValue(), new AddListenerHandler());
//        registerHandler(REMOVE_LISTENER.getValue(), new RemoveListenerHandler());
//        registerHandler(REMOTELY_PROCESS.getValue(), new RemotelyProcessHandler());
//        registerHandler(DESTROY.getValue(), new DestroyHandler());
//        registerHandler(GET_ID.getValue(), new GetIdHandler());
//        registerHandler(ADD_INDEX.getValue(), new AddIndexHandler());
//        registerHandler(NEW_ID.getValue(), new NewIdHandler());
//        registerHandler(EXECUTE.getValue(), new ExecutorServiceHandler());
//        registerHandler(CANCEL_EXECUTION.getValue(), new CancelExecutionHandler());
//        registerHandler(GET_INSTANCES.getValue(), new GetInstancesHandler());
//        registerHandler(GET_MEMBERS.getValue(), new GetMembersHandler());
//        registerHandler(GET_CLUSTER_TIME.getValue(), new GetClusterTimeHandler());
//        registerHandler(CLIENT_AUTHENTICATE.getValue(), new ClientAuthenticateHandler());
//        registerHandler(CLIENT_ADD_INSTANCE_LISTENER.getValue(), new ClientAddInstanceListenerHandler());
//        registerHandler(CLIENT_GET_PARTITIONS.getValue(), new GetPartitionsHandler());
//        registerHandler(ATOMIC_NUMBER_ADD_AND_GET.getValue(), new AtomicLongAddAndGetHandler());
//        registerHandler(ATOMIC_NUMBER_COMPARE_AND_SET.getValue(), new AtomicLongCompareAndSetHandler());
//        registerHandler(ATOMIC_NUMBER_GET_AND_SET.getValue(), new AtomicLongGetAndSetHandler());
//        registerHandler(ATOMIC_NUMBER_GET_AND_ADD.getValue(), new AtomicLongGetAndAddHandler());
//        registerHandler(COUNT_DOWN_LATCH_AWAIT.getValue(), new CountDownLatchAwaitHandler());
//        registerHandler(COUNT_DOWN_LATCH_COUNT_DOWN.getValue(), new CountDownLatchCountDownHandler());
//        registerHandler(COUNT_DOWN_LATCH_GET_COUNT.getValue(), new CountDownLatchGetCountHandler());
//        registerHandler(COUNT_DOWN_LATCH_GET_OWNER.getValue(), new CountDownLatchGetOwnerHandler());
//        registerHandler(COUNT_DOWN_LATCH_SET_COUNT.getValue(), new CountDownLatchSetCountHandler());
//        registerHandler(SEMAPHORE_ATTACH_DETACH_PERMITS.getValue(), new SemaphoreAttachDetachHandler());
//        registerHandler(SEMAPHORE_CANCEL_ACQUIRE.getValue(), new SemaphoreCancelAcquireHandler());
//        registerHandler(SEMAPHORE_DRAIN_PERMITS.getValue(), new SemaphoreDrainHandler());
//        registerHandler(SEMAPHORE_GET_ATTACHED_PERMITS.getValue(), new SemaphoreGetAttachedHandler());
//        registerHandler(SEMAPHORE_GET_AVAILABLE_PERMITS.getValue(), new SemaphoreGetAvailableHandler());
//        registerHandler(SEMAPHORE_REDUCE_PERMITS.getValue(), new SemaphoreReduceHandler());
//        registerHandler(SEMAPHORE_RELEASE.getValue(), new SemaphoreReleaseHandler());
//        registerHandler(SEMAPHORE_TRY_ACQUIRE.getValue(), new SemaphoreTryAcquireHandler());
//        registerHandler(LOCK_LOCK.getValue(), new LockOperationHandler());
//        registerHandler(LOCK_UNLOCK.getValue(), new UnlockOperationHandler());
//        registerHandler(LOCK_FORCE_UNLOCK.getValue(), new UnlockOperationHandler());
//        registerHandler(LOCK_IS_LOCKED.getValue(), new IsLockedOperationHandler());
//        node.connectionManager.addConnectionListener(this);
//        this.THREAD_COUNT = node.getGroupProperties().EXECUTOR_CLIENT_THREAD_COUNT.getInteger();
//        workers = new Worker[THREAD_COUNT];
//        for (int i = 0; i < THREAD_COUNT; i++) {
//            workers[i] = new Worker();
//        }
//        this.factory = node.factory;
//    }

//    void registerHandler(short operation, ClientCommandHandler handler) {
//        clientOperationHandlers[operation] = handler;
//    }
//
//    // always called by InThread
//    public void handle(Packet packet) {
//        if (firstCall) {
//            String threadNamePrefix = node.getThreadPoolNamePrefix("client.service");
//            for (int i = 0; i < THREAD_COUNT; i++) {
//                Worker worker = workers[i];
//                new Thread(node.threadGroup, worker, threadNamePrefix + i).start();
//            }
//            firstCall = false;
//        }
//        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
//        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
//        ClientCommandHandler clientOperationHandler = clientOperationHandlers[packet.operation.getValue()];
//        if (clientOperationHandler == null) {
//            clientOperationHandler = unknownOperationHandler;
//        }
//        if (packet.operation != CLIENT_AUTHENTICATE && !clientEndpoint.isAuthenticated()) {
//            logger.log(Level.SEVERE, "A Client " + packet.conn + " must authenticate before any operation.");
//            node.clientHandlerService.removeClientEndpoint(packet.conn);
//            if (packet.conn != null)
//                packet.conn.close();
//            return;
//        }
//        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, packet, callContext,
//                clientOperationHandler, clientEndpoint.getSubject());
//        clientEndpoint.addRequest(clientRequestHandler);
//        if (packet.operation == CONCURRENT_MAP_UNLOCK) {
//            node.executorManager.executeNow(clientRequestHandler);
//        } else {
//            int hash = hash(callContext.getThreadId(), THREAD_COUNT);
//            workers[hash].addWork(clientRequestHandler);
//        }
//    }
//
//    public void shutdown() {
//        mapClientEndpoints.clear();
//        for (Worker worker : workers) {
//            worker.stop();
//        }
//    }
//
//    public void restart() {
//        for (List<ListenerManager.ListenerItem> listeners : node.listenerManager.namedListeners.values()) {
//            for (ListenerManager.ListenerItem listener : listeners) {
//                if (listener instanceof ClientListener) {
//                    node.listenerManager.removeListener(listener.name, listener, listener.key);
//                }
//            }
//        }
//    }
//
//    private int hash(int id, int maxCount) {
//        return (id == Integer.MIN_VALUE) ? 0 : Math.abs(id) % maxCount;
//    }
//
//    class Worker implements Runnable {
//        private final BlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
//        private volatile boolean active = true;
//
//        public void run() {
//            ThreadContext.get().setCurrentFactory(node.factory);
//            while (active) {
//                Runnable r = null;
//                try {
//                    r = q.take();
//                } catch (InterruptedException e) {
//                    return;
//                }
//                try {
//                    r.run();
//                } catch (Throwable ignored) {
//                }
//            }
//        }
//
//        public void stop() {
//            active = false;
//            q.offer(new Runnable() {
//                public void run() {
//                }
//            });
//        }
//
//        public void addWork(Runnable runnable) {
//            q.offer(runnable);
//        }
//    }
//
//    public int numberOfConnectedClients() {
//        return mapClientEndpoints.size();
//    }
//
//    public ClientEndpoint getClientEndpoint(Connection conn) {
//        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
//        if (clientEndpoint == null) {
//            clientEndpoint = new ClientEndpoint(node, conn);
//            mapClientEndpoints.put(conn, clientEndpoint);
//        }
//        return clientEndpoint;
//    }
//
//    public void connectionAdded(Connection connection) {
//    }
//
//    public void connectionRemoved(final Connection connection) {
//        final ClientEndpoint clientEndpoint = removeClientEndpoint(connection);
//        if (clientEndpoint != null) {
//            node.executorManager.executeNow(new FallThroughRunnable() {
//                public void doRun() {
//                    logger.log(Level.INFO, "Client {" + connection + "} has been removed.");
//                    clientEndpoint.connectionRemoved(connection);
//                    if (node.securityContext != null) {
//                        try {
//                            clientEndpoint.getLoginContext().logout();
//                        } catch (LoginException e) {
//                            logger.log(Level.WARNING, e.getMessage(), e);
//                        }
//                    }
//                }
//            });
//        }
//    }
//
//    ClientEndpoint removeClientEndpoint(final Connection connection) {
//        return mapClientEndpoints.remove(connection);
//    }
//
//    private class QueueOfferHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            long millis = (Long) toObject(value);
//            try {
//                if (millis == -1) {
//                    queue.put(key);
//                    return toData(true);
//                } else if (millis == 0) {
//                    return toData(queue.offer(key));
//                } else {
//                    return toData(queue.offer(key, (Long) toObject(value), TimeUnit.MILLISECONDS));
//                }
//            } catch (InterruptedException e) {
//                throw new RuntimeException();
//            }
//        }
//    }
//
//    private class QueuePollHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            try {
//                long millis = (Long) toObject(value);
//                if (millis == -1) {
//                    return (Data) queue.take();
//                } else if (millis == 0) {
//                    return (Data) queue.poll();
//                } else {
//                    return (Data) queue.poll((Long) millis, TimeUnit.MILLISECONDS);
//                }
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//
//    private class QueueRemoveHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            if (value != null) {
//                return toData(queue.remove(value));
//            } else {
//                return (Data) queue.remove();
//            }
//        }
//    }
//
//    private class QueuePeekHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            return (Data) queue.peek();
//        }
//    }
//
//    private class QueueSizeHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            return toData(queue.size());
//        }
//    }
//
//    private class TopicPublishHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            ITopic<Object> topic = (ITopic) factory.getOrCreateProxyByName(packet.name);
//            topic.publish(packet.getKeyData());
//        }
//
//        @Override
//        protected void sendResponse(Packet request) {
//        }
//    }
//
//    private class QueueRemainingCapacityHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            return toData(queue.remainingCapacity());
//        }
//    }
//
//    private class QueueEntriesHandler extends ClientQueueOperationHandler {
//        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
//            Object[] array = queue.toArray();
//            Keys keys = new Keys();
//            for (Object o : array) {
//                keys.add(toData(o));
//            }
//            return toData(keys);
//        }
//    }
//
//    private class RemotelyProcessHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            node.clusterService.enqueuePacket(packet);
//        }
//
//        @Override
//        protected void sendResponse(Packet request) {
//        }
//    }
//
//    private class DestroyHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            Instance instance = (Instance) factory.getOrCreateProxyByName(packet.name);
//            instance.destroy();
//        }
//    }
//
//    private class NewIdHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            IdGenerator idGen = (IdGenerator) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(idGen.newId()));
//        }
//    }
//
//    private class MapPutMultiHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(multiMap.put(packet.getKeyData(), packet.getValueData())));
//        }
//    }
//
//    private class MapValueCountHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(multiMap.valueCount(packet.getKeyData())));
//        }
//    }
//
//    private class MapRemoveMultiHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//            if (packet.getValueData() == null || packet.getValueData().size() == 0) {
//                MultiMapProxy mmProxy = (MultiMapProxy) multiMap;
//                MProxy mapProxy = mmProxy.getMProxy();
//                packet.setValue((Data) mapProxy.remove(packet.getKeyData()));
//            } else {
//                packet.setValue(toData(multiMap.remove(packet.getKeyData(), packet.getValueData())));
//            }
//        }
//    }
//
//    private class CancelExecutionHandler extends ClientCommandHandler {
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            long taskId = (Long) toObject(packet.getKeyData());
//            boolean mayInterruptIfRunning = (Boolean) toObject(packet.getValue());
//            ClientEndpoint thisEndPoint = getClientEndpoint(packet.conn);
//            DistributedTask task = thisEndPoint.getTask(taskId);
//            boolean cancelled = task.cancel(mayInterruptIfRunning);
//            if (cancelled) {
//                thisEndPoint.removeTask(taskId);
//            }
//            packet.setValue(toData(cancelled));
//        }
//    }
//
//    private class ExecutorServiceHandler extends ClientCommandHandler {
//        @Override
//        public void processCall(Node node, Packet packet) {
//        }
//
//        @Override
//        public final void handle(Node node, final Packet packet) {
//            try {
//                String name = packet.name;
//                ExecutorService executorService = factory.getExecutorService(name);
//                ClientDistributedTask cdt = (ClientDistributedTask) toObject(packet.getKeyData());
//                if (cdt.getMember() != null && cdt.getMember() instanceof HazelcastInstanceAware) {
//                    ((HazelcastInstanceAware) cdt.getMember()).setHazelcastInstance(node.factory);
//                }
//                if (cdt.getMembers() != null) {
//                    Set<Member> set = cdt.getMembers();
//                    for (Member m : set)
//                        if (m instanceof HazelcastInstanceAware)
//                            ((HazelcastInstanceAware) m).setHazelcastInstance(node.factory);
//                }
//                final ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
//                final Callable callable = node.securityContext == null
//                        ? cdt.getCallable()
//                        : node.securityContext.createSecureCallable(clientEndpoint.getSubject(), cdt.getCallable());
//                final DistributedTask task;
//                if (cdt.getKey() != null) {
//                    task = new DistributedTask(callable, cdt.getKey());
//                } else if (cdt.getMember() != null) {
//                    task = new DistributedTask(callable, cdt.getMember());
//                } else if (cdt.getMembers() != null) {
//                    task = new MultiTask(callable, cdt.getMembers());
//                } else {
//                    task = new DistributedTask(callable);
//                }
//                clientEndpoint.storeTask(packet.callId, task);
//                task.setExecutionCallback(new ExecutionCallback() {
//                    public void done(Future future) {
//                        Object result;
//                        try {
//                            clientEndpoint.removeTask(packet.callId);
//                            result = future.get();
//                            packet.setValue(toData(result));
//                        } catch (InterruptedException e) {
//                            return;
//                        } catch (CancellationException e) {
//                            packet.setValue(toData(e));
//                        } catch (ExecutionException e) {
//                            packet.setValue(toData(e));
//                        }
//                        sendResponse(packet);
//                    }
//                });
//                executorService.execute(task);
//            } catch (RuntimeException e) {
//                logger.log(Level.WARNING,
//                        "exception during handling " + packet.operation + ": " + e.getMessage(), e);
//                packet.clearForResponse();
//                packet.setValue(toData(e));
//                sendResponse(packet);
//            }
//        }
//    }
//
//    private class GetInstancesHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            Collection<Instance> instances = factory.getInstances();
//            Keys keys = new Keys();
//            for (Instance instance : instances) {
//                Object id = instance.getId();
//                if (id instanceof FactoryImpl.ProxyKey) {
//                    Object key = ((FactoryImpl.ProxyKey) id).getKey();
//                    if (key instanceof Instance)
//                        id = key.toString();
//                }
//                String idStr = id.toString();
//                if (!idStr.startsWith(Prefix.MAP_OF_LIST) && !idStr.startsWith(Prefix.MAP_FOR_QUEUE)) {
//                    keys.add(toData(id));
//                }
//            }
//            packet.setValue(toData(keys));
//        }
//    }
//
//    private class GetMembersHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            Cluster cluster = factory.getCluster();
//            Set<Member> members = cluster.getMembers();
//            Set<Data> setData = new LinkedHashSet<Data>();
//            if (members != null) {
//                for (Iterator<Member> iterator = members.iterator(); iterator.hasNext(); ) {
//                    Member member = iterator.next();
//                    setData.add(toData(member));
//                }
//                Keys keys = new Keys(setData);
//                packet.setValue(toData(keys));
//            }
//        }
//    }
//
//    private class GetPartitionsHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            PartitionService partitionService = factory.getPartitionService();
//            if (packet.getKeyData() != null && packet.getKeyData().size() > 0) {
//                Object key = toObject(packet.getKeyData());
//                Partition partition = partitionService.getPartition(key);
//                Data value = toData(new PartitionImpl(partition.getPartitionId(), (MemberImpl) partition.getOwner()));
//                packet.setValue(value);
//            } else {
//                Set<Partition> partitions = partitionService.getPartitions();
//                Set<Data> setData = new LinkedHashSet<Data>();
//                for (Iterator<Partition> iterator = partitions.iterator(); iterator.hasNext(); ) {
//                    Partition partition = iterator.next();
//                    setData.add(toData(new PartitionImpl(partition.getPartitionId(), (MemberImpl) partition.getOwner())));
//                }
//                Keys keys = new Keys(setData);
//                packet.setValue(toData(keys));
//            }
//        }
//    }
//
////    abstract private class AtomicLongClientHandler extends ClientCommandHandler {
////        abstract Object processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected);
////
////        public void processCall(Node node, Packet packet) {
////            final AtomicNumberProxy atomicLong = (AtomicNumberProxy) factory.getOrCreateProxyByName(packet.name);
////            final Long value = (Long) toObject(packet.getValueData());
////            final Long expectedValue = (Long) toObject(packet.getKeyData());
////            packet.setValue(toData(processCall(atomicLong, value, expectedValue)));
////        }
////    }
////
////    private class AtomicLongAddAndGetHandler extends AtomicLongClientHandler {
////        public Long processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
////            return atomicLongProxy.addAndGet(value);
////        }
////    }
////
////    private class AtomicLongCompareAndSetHandler extends AtomicLongClientHandler {
////        public Boolean processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
////            return atomicLongProxy.compareAndSet(expected, value);
////        }
////    }
////
////    private class AtomicLongGetAndAddHandler extends AtomicLongClientHandler {
////        public Long processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
////            return atomicLongProxy.getAndAdd(value);
////        }
////    }
////
////    private class AtomicLongGetAndSetHandler extends AtomicLongClientHandler {
////        public Long processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected) {
////            return atomicLongProxy.getAndSet(value);
////        }
////    }
////
////    abstract private class CountDownLatchClientHandler extends ClientCommandHandler {
////        abstract void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value);
////
////        public void processCall(Node node, Packet packet) {
////            final String name = packet.name.substring(Prefix.COUNT_DOWN_LATCH.length());
////            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) factory.getCountDownLatch(name);
////            final Integer value = (Integer) toObject(packet.getValueData());
////            processCall(packet, cdlProxy, value);
////        }
////    }
////
////    private class CountDownLatchAwaitHandler extends CountDownLatchClientHandler {
////        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
////            try {
////                packet.setValue(toData(cdlProxy.await(packet.timeout, TimeUnit.MILLISECONDS)));
////            } catch (Throwable e) {
////                packet.setValue(toData(new ClientServiceException(e)));
////            }
////        }
////    }
////
////    private class CountDownLatchCountDownHandler extends CountDownLatchClientHandler {
////        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
////            cdlProxy.countDown();
////        }
////    }
////
////    private class CountDownLatchGetCountHandler extends CountDownLatchClientHandler {
////        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
////            packet.setValue(toData(cdlProxy.getCount()));
////        }
////    }
////
////    private class CountDownLatchGetOwnerHandler extends CountDownLatchClientHandler {
////        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer value) {
////            packet.setValue(toData(cdlProxy.getOwner()));
////        }
////    }
////
////    private class CountDownLatchSetCountHandler extends CountDownLatchClientHandler {
////        void processCall(Packet packet, CountDownLatchProxy cdlProxy, Integer count) {
////            Address ownerAddress = packet.conn.getEndPoint();
////            packet.setValue(toData(cdlProxy.setCount(count, ownerAddress)));
////        }
////    }
//
//    public static class CountDownLatchLeave implements RemotelyProcessable {
//        Address deadAddress;
//        transient Node node;
//
//        public CountDownLatchLeave(Address deadAddress) {
//            this.deadAddress = deadAddress;
//        }
//
//        public CountDownLatchLeave() {
//        }
//
//        public void setConnection(Connection conn) {
//        }
//
//        public void writeData(DataOutput out) throws IOException {
//            deadAddress.writeData(out);
//        }
//
//        public void readData(DataInput in) throws IOException {
//            (deadAddress = new Address()).readData(in);
//        }
//
//        public Node getNode() {
//            return node;
//        }
//
//        public void setNode(Node node) {
//            this.node = node;
//        }
//
//        public void process() {
//            node.concurrentMapManager.syncForDeadCountDownLatches(deadAddress);
//        }
//    }
//
//    abstract private class SemaphoreClientOperationHandler extends ClientCommandHandler {
//        abstract void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag);
//
//        public void processCall(Node node, Packet packet) {
//            final SemaphoreProxy semaphoreProxy = (SemaphoreProxy) factory.getSemaphore(packet.name);
//            final Integer value = (Integer) toObject(packet.getValueData());
//            final boolean flag = (Boolean) toObject(packet.getKeyData());
//            processCall(packet, semaphoreProxy, value, flag);
//        }
//    }
//
//    private class SemaphoreAttachDetachHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean attach) {
//            ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
//            if (attach) {
//                semaphoreProxy.attach(permits);
//                clientEndpoint.attachDetachPermits(semaphoreProxy.getName(), permits);
//            } else {
//                semaphoreProxy.detach(permits);
//                clientEndpoint.attachDetachPermits(semaphoreProxy.getName(), -permits);
//            }
//        }
//    }
//
//    private class SemaphoreCancelAcquireHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            ConcurrentMapManager.MSemaphore msemaphore = node.concurrentMapManager.new MSemaphore();
//            packet.setValue(toData(msemaphore.cancelAcquire(toData(packet.name))));
//        }
//    }
//
//    private class SemaphoreGetAttachedHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag) {
//            packet.setValue(toData(semaphoreProxy.attachedPermits()));
//        }
//    }
//
//    private class SemaphoreGetAvailableHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag) {
//            packet.setValue(toData(semaphoreProxy.availablePermits()));
//        }
//    }
//
//    private class SemaphoreDrainHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag) {
//            packet.setValue(toData(semaphoreProxy.drainPermits()));
//        }
//    }
//
//    private class SemaphoreReduceHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean flag) {
//            semaphoreProxy.reducePermits(permits);
//        }
//    }
//
//    private class SemaphoreReleaseHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean detach) {
//            if (detach) {
//                semaphoreProxy.releaseDetach(permits);
//                getClientEndpoint(packet.conn).attachDetachPermits(packet.name, -permits);
//            } else {
//                semaphoreProxy.release(permits);
//            }
//        }
//    }
//
//    private class SemaphoreTryAcquireHandler extends SemaphoreClientOperationHandler {
//        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean attach) {
//            try {
//                boolean acquired;
//                if (attach) {
//                    acquired = semaphoreProxy.tryAcquireAttach(permits, packet.timeout, TimeUnit.MILLISECONDS);
//                    if (acquired) {
//                        getClientEndpoint(packet.conn).attachDetachPermits(packet.name, permits);
//                    }
//                } else {
//                    acquired = semaphoreProxy.tryAcquire(permits, packet.timeout, TimeUnit.MILLISECONDS);
//                }
//                packet.setValue(toData(acquired));
//            } catch (Throwable e) {
//                packet.setValue(toData(new ClientServiceException(e)));
//            }
//        }
//    }
//
//    public static class PartitionImpl implements Partition, HazelcastInstanceAware, DataSerializable, Comparable {
//        int partitionId;
//        MemberImpl owner;
//
//        PartitionImpl(int partitionId, MemberImpl owner) {
//            this.partitionId = partitionId;
//            this.owner = owner;
//        }
//
//        public PartitionImpl() {
//        }
//
//        public int getPartitionId() {
//            return partitionId;
//        }
//
//        public Member getOwner() {
//            return owner;
//        }
//
//        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
//            if (owner != null) {
//                owner.setHazelcastInstance(hazelcastInstance);
//            }
//        }
//
//        public void writeData(DataOutput out) throws IOException {
//            out.writeInt(partitionId);
//            boolean hasOwner = (owner != null);
//            out.writeBoolean(hasOwner);
//            if (hasOwner) {
//                owner.writeData(out);
//            }
//        }
//
//        public void readData(DataInput in) throws IOException {
//            partitionId = in.readInt();
//            boolean hasOwner = in.readBoolean();
//            if (hasOwner) {
//                owner = new MemberImpl();
//                owner.readData(in);
//            }
//        }
//
//        public int compareTo(Object o) {
//            PartitionImpl partition = (PartitionImpl) o;
//            Integer id = partitionId;
//            return (id.compareTo(partition.getPartitionId()));
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            PartitionImpl partition = (PartitionImpl) o;
//            return partitionId == partition.partitionId;
//        }
//
//        @Override
//        public int hashCode() {
//            return partitionId;
//        }
//
//        @Override
//        public String toString() {
//            return "Partition [" +
//                    +partitionId +
//                    "], owner=" + owner;
//        }
//    }
//
//    private class GetClusterTimeHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            Cluster cluster = factory.getCluster();
//            long clusterTime = cluster.getClusterTime();
//            packet.setValue(toData(clusterTime));
//        }
//    }
//
//    class ClientAuthenticateHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            final Credentials credentials = (Credentials) toObject(packet.getValueData());
//            boolean authenticated = false;
//            if (credentials == null) {
//                authenticated = false;
//                logger.log(Level.SEVERE, "Could not retrieve Credentials object!");
//            } else if (node.securityContext != null) {
//                final Socket endpointSocket = packet.conn.getSocketChannelWrapper().socket();
//                credentials.setEndpoint(endpointSocket.getInetAddress().getHostAddress());
//                try {
//                    LoginContext lc = node.securityContext.createClientLoginContext(credentials);
//                    lc.login();
//                    getClientEndpoint(packet.conn).setLoginContext(lc);
//                    authenticated = true;
//                } catch (LoginException e) {
//                    logger.log(Level.WARNING, e.getMessage(), e);
//                    authenticated = false;
//                }
//            } else {
//                if (credentials instanceof UsernamePasswordCredentials) {
//                    final UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
//                    final String nodeGroupName = factory.getConfig().getGroupConfig().getName();
//                    final String nodeGroupPassword = factory.getConfig().getGroupConfig().getPassword();
//                    authenticated = (nodeGroupName.equals(usernamePasswordCredentials.getUsername())
//                            && nodeGroupPassword.equals(usernamePasswordCredentials.getPassword()));
//                } else {
//                    authenticated = false;
//                    logger.log(Level.SEVERE, "Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster group-name" +
//                            " and group-password should be used for authentication!\n" +
//                            "Current credentials type is: " + credentials.getClass().getName());
//                }
//            }
//            logger.log((authenticated ? Level.INFO : Level.WARNING), "received auth from " + packet.conn
//                    + ", " + (authenticated ?
//                    "successfully authenticated" : "authentication failed"));
//            packet.clearForResponse();
//            packet.setValue(toData(authenticated));
//            if (!authenticated) {
//                node.clientHandlerService.removeClientEndpoint(packet.conn);
//            } else {
//                ClientEndpoint clientEndpoint = node.clientHandlerService.getClientEndpoint(packet.conn);
//                clientEndpoint.authenticated();
//                Bind bind = new Bind(new Address(packet.conn.getSocketChannelWrapper().socket().getInetAddress(), packet.conn.getSocketChannelWrapper().socket().getPort()));
//                // TODO: client bind !!!
////                bind.setConnection(packet.conn);
////                bind.setNode(node);
////                node.clusterService.enqueueAndWait(bind);
//            }
//        }
//    }
//
//    private class ClientAddInstanceListenerHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            ClientEndpoint endPoint = getClientEndpoint(packet.conn);
//            factory.addInstanceListener(endPoint);
//        }
//    }
//
//    private class GetIdHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return toData(map.getId());
//        }
//    }
//
//    private class AddIndexHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            map.addIndex((String) toObject(key), (Boolean) toObject(value));
//            return null;
//        }
//    }
//
//    private class MapPutAllHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> imap, Data key, Data value) {
//            MProxy mproxy = (MProxy) imap;
//            Pairs pairs = (Pairs) toObject(value);
//            try {
//                node.concurrentMapManager.doPutAll(mproxy.getLongName(), pairs);
//            } catch (Exception e) {
//                logger.log(Level.SEVERE, e.getMessage(), e);
//            }
//            return null;
//        }
//    }
//
//    private class MapPutHandler extends ClientMapOperationHandlerWithTTL {
//
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            MProxy mproxy = (MProxy) map;
//            Object v = value;
//            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
//                v = toObject(value);
//            }
//            if (ttl <= 0) {
//                return (Data) map.put(key, v);
//            } else {
//                return (Data) map.put(key, v, ttl, TimeUnit.MILLISECONDS);
//            }
//        }
//    }
//
//    private class MapPutTransientHandler extends ClientMapOperationHandlerWithTTL {
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            MProxy mproxy = (MProxy) map;
//            Object v = value;
//            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
//                v = toObject(value);
//            }
//            map.putTransient(key, v, ttl, TimeUnit.MILLISECONDS);
//            return null;
//        }
//    }
//
//    private class MapSetHandler extends ClientMapOperationHandlerWithTTL {
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            MProxy mproxy = (MProxy) map;
//            Object v = value;
//            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
//                v = toObject(value);
//            }
//            map.set(key, v, ttl, TimeUnit.MILLISECONDS);
//            return null;
//        }
//    }
//
//    private class MapTryPutHandler extends ClientMapOperationHandlerWithTTL {
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            MProxy mproxy = (MProxy) map;
//            Object v = value;
//            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
//                v = toObject(value);
//            }
//            return toData(map.tryPut(key, v, ttl, TimeUnit.MILLISECONDS));
//        }
//    }
//
//    private class MapPutAndUnlockHandler extends ClientMapOperationHandlerWithTTL {
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            MProxy mproxy = (MProxy) map;
//            Object v = value;
//            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
//                v = toObject(value);
//            }
//            map.putAndUnlock(key, v);
//            return null;
//        }
//    }
//
//    private class MapTryRemoveHandler extends ClientMapOperationHandlerWithTTL {
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            try {
//                return toData(map.tryRemove(key, ttl, TimeUnit.MILLISECONDS));
//            } catch (TimeoutException e) {
//                return toData(new DistributedTimeoutException());
//            }
//        }
//    }
//
//    private class MapTryLockAndGetHandler extends ClientMapOperationHandlerWithTTL {
//        @Override
//        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            try {
//                return toData(map.tryLockAndGet(key, ttl, TimeUnit.MILLISECONDS));
//            } catch (TimeoutException e) {
//                return toData(new DistributedTimeoutException());
//            }
//        }
//    }
//
//    private class MapPutIfAbsentHandler extends ClientCommandHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
//            MProxy mproxy = (MProxy) map;
//            Object v = value;
//            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
//                v = toObject(value);
//            }
//            if (ttl <= 0) {
//                return (Data) map.putIfAbsent(key, v);
//            } else {
//                return (Data) map.putIfAbsent(key, v, ttl, TimeUnit.MILLISECONDS);
//            }
//        }
//
//        public void processCall(Node node, Packet packet) {
//            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            long ttl = packet.timeout;
//            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData(), ttl);
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    private class MapGetAllHandler extends ClientCommandHandler {
//
//        public void processCall(Node node, Packet packet) {
//            Keys keys = (Keys) toObject(packet.getKeyData());
//            Pairs pairs = node.concurrentMapManager.getAllPairs(packet.name, keys);
//            packet.clearForResponse();
//            packet.setValue(toData(pairs));
//        }
//    }
//
//    private class GetMapEntryHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return toData(map.getMapEntry(key));
//        }
//    }
//
//    private class MapGetHandler extends ClientCommandHandler {
//
//        public void processCall(Node node, Packet packet) {
//            InstanceType instanceType = getInstanceType(packet.name);
//            Data key = packet.getKeyData();
//            if (instanceType == InstanceType.MAP) {
//                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//                packet.setKey(null);
//                packet.setValue((Data) map.get(key));
//            } else if (instanceType == InstanceType.MULTIMAP) {
//                MultiMapProxy multiMap = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
//                MProxy mapProxy = multiMap.getMProxy();
//                packet.setKey(null);
//                packet.setValue((Data) mapProxy.get(key));
//            }
//        }
//    }
//
//    private class MapRemoveHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return (Data) map.remove(key);
//        }
//    }
//
//    private class MapRemoveIfSameHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return toData(map.remove(key, value));
//        }
//    }
//
//    private class MapEvictHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return toData(map.evict(key));
//        }
//    }
//
//    private class MapFlushHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            map.flush();
//            return null;
//        }
//    }
//
//    private class MapReplaceIfNotNullHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return (Data) map.replace(key, value);
//        }
//    }
//
//    private class MapReplaceIfSameHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            Keys keys = (Keys) toObject(value);
//            Iterator it = keys.getKeys().iterator();
//            Data expected = (Data) it.next();
//            Data newValue = (Data) it.next();
//            boolean replaced = map.replace(key, expected, newValue);
//            return toData(replaced);
//        }
//    }
//
//    private class MapContainsHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            InstanceType instanceType = getInstanceType(packet.name);
//            if (instanceType.equals(InstanceType.MAP)) {
//                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//                packet.setValue(toData(map.containsKey(packet.getKeyData())));
//            } else if (instanceType.equals(InstanceType.LIST) || instanceType.equals(InstanceType.SET)) {
//                Collection<Object> collection = (Collection) factory.getOrCreateProxyByName(packet.name);
//                packet.setValue(toData(collection.contains(packet.getKeyData())));
//            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
//                MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//                packet.setValue(toData(multiMap.containsKey(packet.getKeyData())));
//            }
//        }
//    }
//
//    private class MapContainsValueHandler extends ClientCommandHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            return toData(map.containsValue(value));
//        }
//
//        public void processCall(Node node, Packet packet) {
//            InstanceType instanceType = getInstanceType(packet.name);
//            if (instanceType.equals(InstanceType.MAP)) {
//                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//                packet.setValue(toData(map.containsValue(packet.getValueData())));
//            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
//                MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//                if (packet.getKeyData() != null && packet.getKeyData().size() > 0) {
//                    packet.setValue(toData(multiMap.containsEntry(packet.getKeyData(), packet.getValueData())));
//                } else {
//                    packet.setValue(toData(multiMap.containsValue(packet.getValueData())));
//                }
//            }
//        }
//    }
//
//    private class MapSizeHandler extends ClientCollectionOperationHandler {
//        @Override
//        public void doListOp(Node node, Packet packet) {
//            IList<Object> list = (IList) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(list.size()));
//        }
//
//        @Override
//        public void doMapOp(Node node, Packet packet) {
//            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(map.size()));
//        }
//
//        @Override
//        public void doSetOp(Node node, Packet packet) {
//            ISet<Object> set = (ISet) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(set.size()));
//        }
//
//        @Override
//        public void doMultiMapOp(Node node, Packet packet) {
//            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(multiMap.size()));
//        }
//
//        @Override
//        public void doQueueOp(Node node, Packet packet) {
//            //ignore
//        }
//    }
//
//    private class MapLockHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            throw new RuntimeException("Shouldn't invoke this method");
//        }
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            Instance.InstanceType type = getInstanceType(packet.name);
//            long timeout = packet.timeout;
//            Data value = null;
//            IMap<Object, Object> map = null;
//            if (type == Instance.InstanceType.MAP) {
//                map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            } else {
//                MultiMapProxy multiMapProxy = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
//                map = multiMapProxy.getMProxy();
//            }
//            if (timeout == -1) {
//                map.lock(packet.getKeyData());
//                value = null;
//            } else if (timeout == 0) {
//                value = toData(map.tryLock(packet.getKeyData()));
//            } else {
//                TimeUnit timeUnit = (TimeUnit) toObject(packet.getValueData());
//                timeUnit = timeUnit == null ? TimeUnit.MILLISECONDS : timeUnit;
//                value = toData(map.tryLock(packet.getKeyData(), timeout, timeUnit));
//            }
//            packet.setValue(value);
//        }
//    }
//
//    private class MapIsKeyLockedHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            throw new RuntimeException("Shouldn't invoke this method");
//        }
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            IMap<Object, Object> map = null;
//            map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            packet.setValue(toData(map.isLocked(packet.getKeyData())));
//        }
//    }
//
//    private class MapForceUnlockHandler extends MapUnlockHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            map.forceUnlock(key);
//            return null;
//        }
//    }
//
//    private class MapUnlockHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            map.unlock(key);
//            return null;
//        }
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            Instance.InstanceType type = getInstanceType(packet.name);
//            IMap<Object, Object> map = null;
//            if (type == Instance.InstanceType.MAP) {
//                map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            } else {
//                MultiMapProxy multiMapProxy = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
//                map = multiMapProxy.getMProxy();
//            }
//            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    private class MapLockMapHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            throw new RuntimeException("Shouldn't invoke this method");
//        }
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            long timeout = packet.timeout;
//            TimeUnit timeUnit = (TimeUnit) toObject(packet.getValueData());
//            timeUnit = (timeUnit == null) ? TimeUnit.MILLISECONDS : timeUnit;
//            Data value = toData(map.lockMap(timeout, timeUnit));
//            packet.setValue(value);
//        }
//    }
//
//    private class MapUnlockMapHandler extends ClientMapOperationHandler {
//        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
//            map.unlockMap();
//            return null;
//        }
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
//    private class LockOperationHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            final Object key = toObject(packet.getKeyData());
//            final ILock lock = factory.getLock(key);
//            final long timeout = packet.timeout;
//            Data value = null;
//            if (timeout == -1) {
//                lock.lock();
//            } else if (timeout == 0) {
//                value = toData(lock.tryLock());
//            } else {
//                try {
//                    value = toData(lock.tryLock(timeout, TimeUnit.MILLISECONDS));
//                } catch (InterruptedException e) {
//                    logger.log(Level.FINEST, "Lock interrupted!");
//                    value = toData(Boolean.FALSE);
//                }
//            }
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    private class IsLockedOperationHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            final Object key = toObject(packet.getKeyData());
//            final ILock lock = factory.getLock(key);
//            Data value = null;
//            value = toData(lock.isLocked());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    private class UnlockOperationHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            final Object key = toObject(packet.getKeyData());
//            final ILock lock = factory.getLock(key);
//            if (packet.operation == LOCK_UNLOCK) {
//                lock.unlock();
//            } else if (packet.operation == LOCK_FORCE_UNLOCK) {
//                lock.forceUnlock();
//            }
//            packet.clearForResponse();
//            packet.setValue(null);
//        }
//    }
//
//    private class TransactionBeginHandler extends ClientTransactionOperationHandler {
//        public void processTransactionOp(Transaction transaction) {
//            transaction.begin();
//        }
//    }
//
//    private class TransactionCommitHandler extends ClientTransactionOperationHandler {
//        public void processTransactionOp(Transaction transaction) {
//            transaction.commit();
//        }
//    }
//
//    private class TransactionRollbackHandler extends ClientTransactionOperationHandler {
//        public void processTransactionOp(Transaction transaction) {
//            transaction.rollback();
//        }
//    }
//
//    private class MapIterateEntriesHandler extends ClientCollectionOperationHandler {
//        public Data getMapKeys(final IMap<Object, Object> map, final Data key, final Data value) {
//            Entries entries = null;
//            if (value == null) {
//                entries = (Entries) map.entrySet();
//            } else {
//                final Predicate p = (Predicate) toObject(value);
//                entries = (Entries) map.entrySet(p);
//            }
//            final Collection<Map.Entry> colEntries = entries.getKeyValues();
//            final Keys keys = new Keys(new ArrayList<Data>(colEntries.size() << 1));
//            for (final Object obj : colEntries) {
//                final KeyValue entry = (KeyValue) obj;
//                keys.add(toData(entry));
//            }
//            return toData(keys);
//        }
//
//        @Override
//        public void doListOp(final Node node, final Packet packet) {
//            IMap mapProxy = (IMap) factory.getOrCreateProxyByName(Prefix.MAP + Prefix.QUEUE + packet.name);
//            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData()));
//        }
//
//        @Override
//        public void doMapOp(final Node node, final Packet packet) {
//            packet.setValue(getMapKeys((IMap) factory.getOrCreateProxyByName(packet.name), packet.getKeyData(), packet.getValueData()));
//        }
//
//        @Override
//        public void doSetOp(final Node node, final Packet packet) {
//            final SetProxy collectionProxy = (SetProxy) factory.getOrCreateProxyByName(packet.name);
//            final MProxy mapProxy = collectionProxy.getMProxy();
//            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData()));
//        }
//
//        @Override
//        public void doMultiMapOp(final Node node, final Packet packet) {
//            final MultiMapProxy multiMap = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
//            final MProxy mapProxy = multiMap.getMProxy();
//            final Data value = getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//
//        @Override
//        public void doQueueOp(final Node node, final Packet packet) {
//            final IQueue queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
//        }
//    }
//
//    private class MapIterateKeysHandler extends ClientCollectionOperationHandler {
//        public Data getMapKeys(IMap<Object, Object> map, Data key, Data value, Collection<Data> collection) {
//            Entries entries = null;
//            if (value == null) {
//                entries = (Entries) map.keySet();
//            } else {
//                Predicate p = (Predicate) toObject(value);
//                entries = (Entries) map.keySet(p);
//            }
//            Collection<Map.Entry> colEntries = entries.getKeyValues();
//            Keys keys = new Keys(collection);
//            for (Object obj : colEntries) {
//                KeyValue entry = (KeyValue) obj;
//                keys.add(entry.getKeyData());
//            }
//            return toData(keys);
//        }
//
//        @Override
//        public void doListOp(Node node, Packet packet) {
//            IMap mapProxy = (IMap) factory.getOrCreateProxyByName(Prefix.MAP + Prefix.QUEUE + packet.name);
//            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new ArrayList<Data>()));
//        }
//
//        @Override
//        public void doMapOp(Node node, Packet packet) {
//            packet.setValue(getMapKeys((IMap) factory.getOrCreateProxyByName(packet.name), packet.getKeyData(), packet.getValueData(), new HashSet<Data>()));
//        }
//
//        @Override
//        public void doSetOp(Node node, Packet packet) {
//            SetProxy collectionProxy = (SetProxy) factory.getOrCreateProxyByName(packet.name);
//            MProxy mapProxy = collectionProxy.getMProxy();
//            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new HashSet<Data>()));
//        }
//
//        public void doMultiMapOp(Node node, Packet packet) {
//            MultiMapProxy multiMap = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
//            MProxy mapProxy = multiMap.getMProxy();
//            Data value = getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new HashSet<Data>());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//
//        public void doQueueOp(Node node, Packet packet) {
//            IQueue queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
//        }
//    }
//
//    private class AddListenerHandler extends ClientCommandHandler {
//        public void processCall(Node node, final Packet packet) {
//            final ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
//            boolean includeValue = (int) packet.longValue == 1;
////            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
////                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
////                clientEndpoint.addThisAsListener(map, packet.getKeyData(), includeValue);
////            } else if (getInstanceType(packet.name).equals(InstanceType.MULTIMAP)) {
////                MultiMap<Object, Object> multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
////                clientEndpoint.addThisAsListener(multiMap, packet.getKeyData(), includeValue);
////            } else if (getInstanceType(packet.name).equals(InstanceType.LIST)) {
////                ListProxyImpl listProxy = (ListProxyImpl) factory.getOrCreateProxyByName(packet.name);
////                IMap map = (IMap) factory.getOrCreateProxyByName(Prefix.MAP + (String) listProxy.getId());
////                clientEndpoint.addThisAsListener(map, null, includeValue);
////            } else if (getInstanceType(packet.name).equals(InstanceType.SET)) {
////                SetProxy collectionProxy = (SetProxy) factory.getOrCreateProxyByName(packet.name);
////                IMap map = collectionProxy.getMProxy();
////                clientEndpoint.addThisAsListener(map, null, includeValue);
////            } else if (getInstanceType(packet.name).equals(InstanceType.QUEUE)) {
////                IQueue<Object> queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
////                final String packetName = packet.name;
////                ItemListener itemListener = new ClientItemListener(clientEndpoint, packetName);
////                queue.addItemListener(itemListener, includeValue);
////                clientEndpoint.queueItemListeners.put(queue, itemListener);
////            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
////                ITopic<Object> topic = (ITopic) factory.getOrCreateProxyByName(packet.name);
////                final String packetName = packet.name;
////                MessageListener messageListener = new ClientMessageListener(clientEndpoint, packetName);
////                topic.addMessageListener(messageListener);
////                clientEndpoint.messageListeners.put(topic, messageListener);
////            }
//            packet.clearForResponse();
//        }
//    }
//
//    public interface ClientListener {
//
//    }
//
//    public class ClientItemListener implements ItemListener, ClientListener {
//        final ClientEndpoint clientEndpoint;
//        final String name;
//
//        ClientItemListener(ClientEndpoint clientEndpoint, String name) {
//            this.clientEndpoint = clientEndpoint;
//            this.name = name;
//        }
//
//        public void itemAdded(ItemEvent itemEvent) {
//            Packet p = new Packet();
//            DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) itemEvent;
//            p.set(name, ClusterOperation.EVENT, dataAwareItemEvent.getItemData(), true);
//            clientEndpoint.sendPacket(p);
//        }
//
//        public void itemRemoved(ItemEvent itemEvent) {
//            Packet p = new Packet();
//            DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) itemEvent;
//            p.set(name, ClusterOperation.EVENT, dataAwareItemEvent.getItemData(), false);
//            clientEndpoint.sendPacket(p);
//        }
//    }
//
//    public class ClientMessageListener implements MessageListener, ClientListener {
//        final ClientEndpoint clientEndpoint;
//        final String name;
//
//        ClientMessageListener(ClientEndpoint clientEndpoint, String name) {
//            this.clientEndpoint = clientEndpoint;
//            this.name = name;
//        }
//
//        public void onMessage(Message msg) {
//            Packet p = new Packet();
//            DataMessage dataMessage = (DataMessage) msg;
//            p.set(name, ClusterOperation.EVENT, dataMessage.getMessageData(), null);
//            clientEndpoint.sendPacket(p);
//        }
//    }
//
//    private class RemoveListenerHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
//            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
//                IMap map = (IMap) factory.getOrCreateProxyByName(packet.name);
//                clientEndpoint.removeThisListener(map, packet.getKeyData());
//            }
//            if (getInstanceType(packet.name).equals(InstanceType.MULTIMAP)) {
//                MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
//                clientEndpoint.removeThisListener(multiMap, packet.getKeyData());
//            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
//                ITopic topic = (ITopic) factory.getOrCreateProxyByName(packet.name);
//                topic.removeMessageListener(clientEndpoint.messageListeners.remove(topic));
//            } else if (getInstanceType(packet.name).equals(InstanceType.QUEUE)) {
//                IQueue queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
//                queue.removeItemListener(clientEndpoint.queueItemListeners.remove(queue));
//            }
//        }
//    }
//
//    private class ListAddHandler extends ClientCommandHandler {
//        @Override
//        public void processCall(Node node, Packet packet) {
//            IList list = (IList) factory.getOrCreateProxyByName(packet.name);
//            Boolean value = list.add(packet.getKeyData());
//            packet.clearForResponse();
//            packet.setValue(toData(value));
//        }
//    }
//
//    private class SetAddHandler extends ClientCommandHandler {
//        @Override
//        public void processCall(Node node, Packet packet) {
//            ISet list = (ISet) factory.getOrCreateProxyByName(packet.name);
//            boolean value = list.add(packet.getKeyData());
//            packet.clearForResponse();
//            packet.setValue(toData(value));
//        }
//    }
//
//    private class MapItemRemoveHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            Collection collection = (Collection) factory.getOrCreateProxyByName(packet.name);
//            Data value = toData(collection.remove(packet.getKeyData()));
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    public abstract class ClientCommandHandler {
//        public abstract void processCall(Node node, Packet packet);
//
//        public void handle(Node node, Packet packet) {
//            try {
//                processCall(node, packet);
//            } catch (RuntimeException e) {
//                logger.log(Level.WARNING,
//                        "exception during handling " + packet.operation + ": " + e.getMessage(), e);
//                packet.clearForResponse();
//                packet.setValue(toData(new ClientServiceException(e)));
//            }
//            sendResponse(packet);
//        }
//
//        protected void sendResponse(Packet request) {
//            request.lockAddress = null;
//            request.operation = RESPONSE;
//            request.responseType = RESPONSE_SUCCESS;
//            if (request.conn != null && request.conn.live()) {
//                request.conn.getWriteHandler().enqueueSocketWritable(request);
//            } else {
//                logger.log(Level.WARNING, "unable to send response " + request);
//            }
//        }
//    }
//
//    private final class UnknownClientOperationHandler extends ClientCommandHandler {
//        public void processCall(Node node, Packet packet) {
//            final String error = "Unknown Client Operation, can not handle " + packet.operation;
//            if (node.isActive()) {
//                throw new RuntimeException(error);
//            } else {
//                logger.log(Level.WARNING, error);
//            }
//        }
//    }
//
//    private abstract class ClientMapOperationHandler extends ClientCommandHandler {
//        public abstract Data processMapOp(IMap<Object, Object> map, Data key, Data value);
//
//        public void processCall(Node node, Packet packet) {
//            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    private abstract class ClientMapOperationHandlerWithTTL extends ClientCommandHandler {
//        public void processCall(Node node, final Packet packet) {
//            final IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
//            final long ttl = packet.timeout;
//            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData(), ttl);
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//
//        protected abstract Data processMapOp(IMap<Object, Object> map, Data keyData, Data valueData, long ttl);
//    }
//
//    private abstract class ClientQueueOperationHandler extends ClientCommandHandler {
//        public abstract Data processQueueOp(IQueue<Object> queue, Data key, Data value);
//
//        public void processCall(Node node, Packet packet) {
//            IQueue<Object> queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
//            Data value = processQueueOp(queue, packet.getKeyData(), packet.getValueData());
//            packet.clearForResponse();
//            packet.setValue(value);
//        }
//    }
//
//    private abstract class ClientCollectionOperationHandler extends ClientCommandHandler {
//        public abstract void doMapOp(Node node, Packet packet);
//
//        public abstract void doListOp(Node node, Packet packet);
//
//        public abstract void doSetOp(Node node, Packet packet);
//
//        public abstract void doMultiMapOp(Node node, Packet packet);
//
//        public abstract void doQueueOp(Node node, Packet packet);
//
//        @Override
//        public void processCall(Node node, Packet packet) {
//            InstanceType instanceType = getInstanceType(packet.name);
//            if (instanceType.equals(InstanceType.LIST)) {
//                doListOp(node, packet);
//            } else if (instanceType.equals(InstanceType.SET)) {
//                doSetOp(node, packet);
//            } else if (instanceType.equals(InstanceType.MAP)) {
//                doMapOp(node, packet);
//            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
//                doMultiMapOp(node, packet);
//            } else if (instanceType.equals(InstanceType.QUEUE)) {
//                doQueueOp(node, packet);
//            }
//        }
//    }
//
//    private abstract class ClientTransactionOperationHandler extends ClientCommandHandler {
//        public abstract void processTransactionOp(Transaction transaction);
//
//        public void processCall(Node node, Packet packet) {
//            Transaction transaction = factory.getTransaction();
//            processTransactionOp(transaction);
//        }
//    }
//
//    private class ClientServiceMembershipListener implements MembershipListener {
//        public void memberAdded(MembershipEvent membershipEvent) {
//            notifyEndPoints(membershipEvent);
//        }
//
//        public void memberRemoved(MembershipEvent membershipEvent) {
//            notifyEndPoints(membershipEvent);
//        }
//
//        void notifyEndPoints(MembershipEvent membershipEvent) {
//            for (ClientEndpoint endpoint : mapClientEndpoints.values()) {
//                Packet membershipEventPacket = endpoint.createMembershipEventPacket(membershipEvent);
//                endpoint.sendPacket(membershipEventPacket);
//            }
//        }
//    }
//}

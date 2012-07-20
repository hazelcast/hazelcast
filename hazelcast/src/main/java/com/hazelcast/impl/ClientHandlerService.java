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

package com.hazelcast.impl;

import com.hazelcast.cluster.Bind;
import com.hazelcast.cluster.RemotelyProcessable;
import com.hazelcast.core.*;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.util.DistributedTimeoutException;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.impl.BaseManager.getInstanceType;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ClientHandlerService implements ConnectionListener {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new ConcurrentHashMap<Connection, ClientEndpoint>();
    private final ClientOperationHandler[] clientOperationHandlers = new ClientOperationHandler[ClusterOperation.LENGTH];
    private final Map<String, CommandHandler> mapCommandHandlers = new HashMap<String, CommandHandler>();
    private final ClientOperationHandler unknownOperationHandler = new UnknownClientOperationHandler();
    private final ILogger logger;
    private final int THREAD_COUNT;
    final Worker[] workers;
    private final FactoryImpl factory;
    boolean firstCall = true;

    public ClientHandlerService(Node node) {
        this.node = node;
        this.logger = node.getLogger(this.getClass().getName());
        node.getClusterImpl().addMembershipListener(new ClientServiceMembershipListener());
        mapCommandHandlers.put(Command.AUTH.value, new ClientAuthenticateHandler());
        mapCommandHandlers.put(Command.MGET.value, new MapGetHandler());
        mapCommandHandlers.put(Command.MGETALL.value, new MapGetAllHandler());
        mapCommandHandlers.put(Command.MPUT.value, new MapPutHandler());
        mapCommandHandlers.put(Command.MTRYPUT.value, new MapTryPutHandler());
        mapCommandHandlers.put(Command.MSET.value, new MapSetHandler());
        mapCommandHandlers.put(Command.MPUTTRANSIENT.value, new MapPutTransientHandler());
        mapCommandHandlers.put(Command.MPUTANDUNLOCK.value, new MapPutAndUnlockHandler());
        mapCommandHandlers.put(Command.MTRYLOCKANDGET.value, new MapTryLockAndGetHandler());
        mapCommandHandlers.put(Command.MADDLISTENER.value, new MapAddListenerHandler());
        mapCommandHandlers.put(Command.MREMOVELISTENER.value, new MapRemoveListenerHandler());
        mapCommandHandlers.put(Command.ADDLISTENER.value, new AddListenerHandler());
        mapCommandHandlers.put(Command.REMOVELISTENER.value, new RemoveListenerHandler());
        mapCommandHandlers.put(Command.KEYSET.value, new MapIterateKeysHandler());
        mapCommandHandlers.put(Command.ENTRYSET.value, new MapIterateEntriesHandler());
        mapCommandHandlers.put(Command.MGETENTRY.value, new GetMapEntryHandler());
        mapCommandHandlers.put(Command.MLOCK.value, new MapLockHandler());
        mapCommandHandlers.put(Command.MTRYLOCK.value, new MapLockHandler());
        mapCommandHandlers.put(Command.MTRYREMOVE.value, new MapTryRemoveHandler());
        mapCommandHandlers.put(Command.MLOCKMAP.value, new MapLockMapHandler());
        mapCommandHandlers.put(Command.MUNLOCKMAP.value, new MapUnlockMapHandler());
        mapCommandHandlers.put(Command.MFORCEUNLOCK.value, new MapForceUnlockHandler());
        mapCommandHandlers.put(Command.MISKEYLOCKED.value, new MapIsKeyLockedHandler());
        mapCommandHandlers.put(Command.MUNLOCK.value, new MapUnlockHandler());
        mapCommandHandlers.put(Command.MPUTALL.value, new MapPutAllHandler());
        mapCommandHandlers.put(Command.MREMOVE.value, new MapRemoveHandler());
        mapCommandHandlers.put(Command.MREMOVEITEM.value, new MapItemRemoveHandler());
        mapCommandHandlers.put(Command.MCONTAINSKEY.value, new MapContainsHandler());
        mapCommandHandlers.put(Command.MCONTAINSVALUE.value, new MapContainsValueHandler());
        mapCommandHandlers.put(Command.MPUTIFABSENT.value, new MapPutIfAbsentHandler());
        mapCommandHandlers.put(Command.MREMOVEIFSAME.value, new MapRemoveIfSameHandler());
        mapCommandHandlers.put(Command.MREPLACEIFNOTNULL.value, new MapReplaceIfNotNullHandler());
        mapCommandHandlers.put(Command.MREPLACEIFSAME.value, new MapReplaceIfSameHandler());
        mapCommandHandlers.put(Command.MFLUSH.value, new MapFlushHandler());
        mapCommandHandlers.put(Command.MMPUT.value, new MapPutMultiHandler());
        mapCommandHandlers.put(Command.MMREMOVE.value, new MapRemoveMultiHandler());
        mapCommandHandlers.put(Command.MMVALUECOUNT.value, new MapValueCountHandler());
        mapCommandHandlers.put(Command.MEVICT.value, new MapEvictHandler());
        mapCommandHandlers.put(Command.MFLUSH.value, new MapFlushHandler());
        mapCommandHandlers.put(Command.SADD.value, new SetAddHandler());
        mapCommandHandlers.put(Command.LADD.value, new ListAddHandler());
        mapCommandHandlers.put(Command.ADDANDGET.value, new AtomicLongAddAndGetHandler());
        mapCommandHandlers.put(Command.COMPAREANDSET.value, new AtomicLongCompareAndSetHandler());
        mapCommandHandlers.put(Command.GETANDSET.value, new AtomicLongGetAndSetHandler());
        mapCommandHandlers.put(Command.GETANDADD.value, new AtomicLongGetAndAddHandler());
        mapCommandHandlers.put(Command.QOFFER.value, new QueueOfferHandler());
        mapCommandHandlers.put(Command.QPOLL.value, new QueuePollHandler());
        mapCommandHandlers.put(Command.QSIZE.value, new QueueSizeHandler());
        mapCommandHandlers.put(Command.QPEEK.value, new QueuePeekHandler());
        mapCommandHandlers.put(Command.QREMOVE.value, new QueueRemoveHandler());
        mapCommandHandlers.put(Command.QREMCAPACITY.value, new QueueRemainingCapacityHandler());
        mapCommandHandlers.put(Command.QENTRIES.value, new QueueEntriesHandler());
        mapCommandHandlers.put(Command.QADDLISTENER.value, new QueueAddListenerHandler());
        mapCommandHandlers.put(Command.QREMOVELISTENER.value, new QueueRemoveListenerHandler());
        mapCommandHandlers.put(Command.TRXBEGIN.value, new TransactionBeginHandler());
        mapCommandHandlers.put(Command.TRXCOMMIT.value, new TransactionCommitHandler());
        mapCommandHandlers.put(Command.TRXROLLBACK.value, new TransactionRollbackHandler());
        mapCommandHandlers.put(Command.NEWID.value, new NewIdHandler());
        mapCommandHandlers.put(Command.INSTANCES.value, new GetInstancesHandler());
        mapCommandHandlers.put(Command.MEMBERS.value, new GetMembersHandler());
        mapCommandHandlers.put(Command.CLUSTERTIME.value, new GetClusterTimeHandler());
        mapCommandHandlers.put(Command.PARTITIONS.value, new GetPartitionsHandler());
        mapCommandHandlers.put(Command.CDLAWAIT.value, new CountDownLatchAwaitHandler());
        mapCommandHandlers.put(Command.CDLCOUNTDOWN.value, new CountDownLatchCountDownHandler());
        mapCommandHandlers.put(Command.CDLGETCOUNT.value, new CountDownLatchGetCountHandler());
        mapCommandHandlers.put(Command.CDLGETOWNER.value, new CountDownLatchGetOwnerHandler());
        mapCommandHandlers.put(Command.CDLSETCOUNT.value, new CountDownLatchSetCountHandler());
        mapCommandHandlers.put(Command.LOCK_LOCK.value, new LockOperationHandler());
        mapCommandHandlers.put(Command.LOCK_TRYLOCK.value, new LockOperationHandler());
        mapCommandHandlers.put(Command.LOCK_UNLOCK.value, new UnlockOperationHandler());
        mapCommandHandlers.put(Command.LOCK_FORCE_UNLOCK.value, new UnlockOperationHandler());
        mapCommandHandlers.put(Command.LOCK_IS_LOCKED.value, new IsLockedOperationHandler());
        mapCommandHandlers.put(Command.TPUBLISH.value, new TopicPublishHandler());

//        SEMATTACHDETACHPERMITS, SEMCANCELACQUIRE, SEMDESTROY, SEM_DRAIN_PERMITS, SEMGETATTACHEDPERMITS,
//                SEMGETAVAILPERMITS, SEMREDUCEPERMITS, SEMRELEASE, SEMTRYACQUIRE,




        registerHandler(CONCURRENT_MAP_PUT.getValue(), new MapPutHandler());
        registerHandler(CONCURRENT_MAP_PUT_AND_UNLOCK.getValue(), new MapPutAndUnlockHandler());
        registerHandler(CONCURRENT_MAP_PUT_ALL.getValue(), new MapPutAllHandler());
        registerHandler(CONCURRENT_MAP_PUT_MULTI.getValue(), new MapPutMultiHandler());
        registerHandler(CONCURRENT_MAP_PUT_IF_ABSENT.getValue(), new MapPutIfAbsentHandler());
        registerHandler(CONCURRENT_MAP_PUT_TRANSIENT.getValue(), new MapPutTransientHandler());
        registerHandler(CONCURRENT_MAP_SET.getValue(), new MapSetHandler());
        registerHandler(CONCURRENT_MAP_TRY_PUT.getValue(), new MapTryPutHandler());
        registerHandler(CONCURRENT_MAP_GET.getValue(), new MapGetHandler());
        registerHandler(CONCURRENT_MAP_GET_ALL.getValue(), new MapGetAllHandler());
        registerHandler(CONCURRENT_MAP_REMOVE.getValue(), new MapRemoveHandler());
        registerHandler(CONCURRENT_MAP_TRY_REMOVE.getValue(), new MapTryRemoveHandler());
        registerHandler(CONCURRENT_MAP_REMOVE_IF_SAME.getValue(), new MapRemoveIfSameHandler());
        registerHandler(CONCURRENT_MAP_REMOVE_MULTI.getValue(), new MapRemoveMultiHandler());
        registerHandler(CONCURRENT_MAP_EVICT.getValue(), new MapEvictHandler());
        registerHandler(CONCURRENT_MAP_FLUSH.getValue(), new MapFlushHandler());
        registerHandler(CONCURRENT_MAP_REPLACE_IF_NOT_NULL.getValue(), new MapReplaceIfNotNullHandler());
        registerHandler(CONCURRENT_MAP_REPLACE_IF_SAME.getValue(), new MapReplaceIfSameHandler());
        registerHandler(CONCURRENT_MAP_SIZE.getValue(), new MapSizeHandler());
        registerHandler(CONCURRENT_MAP_GET_MAP_ENTRY.getValue(), new GetMapEntryHandler());
        registerHandler(CONCURRENT_MAP_TRY_LOCK_AND_GET.getValue(), new MapTryLockAndGetHandler());
        registerHandler(CONCURRENT_MAP_LOCK.getValue(), new MapLockHandler());
        registerHandler(CONCURRENT_MAP_IS_KEY_LOCKED.getValue(), new MapIsKeyLockedHandler());
        registerHandler(CONCURRENT_MAP_UNLOCK.getValue(), new MapUnlockHandler());
        registerHandler(CONCURRENT_MAP_FORCE_UNLOCK.getValue(), new MapForceUnlockHandler());
        registerHandler(CONCURRENT_MAP_LOCK_MAP.getValue(), new MapLockMapHandler());
        registerHandler(CONCURRENT_MAP_UNLOCK_MAP.getValue(), new MapUnlockMapHandler());
        registerHandler(CONCURRENT_MAP_CONTAINS_KEY.getValue(), new MapContainsHandler());
        registerHandler(CONCURRENT_MAP_CONTAINS_VALUE.getValue(), new MapContainsValueHandler());
        registerHandler(CONCURRENT_MAP_ADD_TO_LIST.getValue(), new ListAddHandler());
        registerHandler(CONCURRENT_MAP_ADD_TO_SET.getValue(), new SetAddHandler());
        registerHandler(CONCURRENT_MAP_REMOVE_ITEM.getValue(), new MapItemRemoveHandler());
        registerHandler(CONCURRENT_MAP_ITERATE_KEYS.getValue(), new MapIterateKeysHandler());
        registerHandler(CONCURRENT_MAP_ITERATE_ENTRIES.getValue(), new MapIterateEntriesHandler());
        registerHandler(CONCURRENT_MAP_VALUE_COUNT.getValue(), new MapValueCountHandler());
        registerHandler(TOPIC_PUBLISH.getValue(), new TopicPublishHandler());
        registerHandler(BLOCKING_QUEUE_OFFER.getValue(), new QueueOfferHandler());
        registerHandler(BLOCKING_QUEUE_POLL.getValue(), new QueuePollHandler());
        registerHandler(BLOCKING_QUEUE_REMOVE.getValue(), new QueueRemoveHandler());
        registerHandler(BLOCKING_QUEUE_PEEK.getValue(), new QueuePeekHandler());
        registerHandler(BLOCKING_QUEUE_SIZE.getValue(), new QueueSizeHandler());
        registerHandler(BLOCKING_QUEUE_REMAINING_CAPACITY.getValue(), new QueueRemainingCapacityHandler());
        registerHandler(BLOCKING_QUEUE_ENTRIES.getValue(), new QueueEntriesHandler());
        registerHandler(TRANSACTION_BEGIN.getValue(), new TransactionBeginHandler());
        registerHandler(TRANSACTION_COMMIT.getValue(), new TransactionCommitHandler());
        registerHandler(TRANSACTION_ROLLBACK.getValue(), new TransactionRollbackHandler());
        registerHandler(ADD_LISTENER.getValue(), new AddListenerHandler());
        registerHandler(REMOVE_LISTENER.getValue(), new RemoveListenerHandler());
        registerHandler(REMOTELY_PROCESS.getValue(), new RemotelyProcessHandler());
        registerHandler(DESTROY.getValue(), new DestroyHandler());
        registerHandler(GET_ID.getValue(), new GetIdHandler());
        registerHandler(ADD_INDEX.getValue(), new AddIndexHandler());
        registerHandler(NEW_ID.getValue(), new NewIdHandler());
        registerHandler(EXECUTE.getValue(), new ExecutorServiceHandler());
        registerHandler(CANCEL_EXECUTION.getValue(), new CancelExecutionHandler());
        registerHandler(GET_INSTANCES.getValue(), new GetInstancesHandler());
        registerHandler(GET_MEMBERS.getValue(), new GetMembersHandler());
        registerHandler(GET_CLUSTER_TIME.getValue(), new GetClusterTimeHandler());
        registerHandler(CLIENT_AUTHENTICATE.getValue(), new ClientAuthenticateHandler());
        registerHandler(CLIENT_ADD_INSTANCE_LISTENER.getValue(), new ClientAddInstanceListenerHandler());
        registerHandler(CLIENT_GET_PARTITIONS.getValue(), new GetPartitionsHandler());
        registerHandler(ATOMIC_NUMBER_ADD_AND_GET.getValue(), new AtomicLongAddAndGetHandler());
        registerHandler(ATOMIC_NUMBER_COMPARE_AND_SET.getValue(), new AtomicLongCompareAndSetHandler());
        registerHandler(ATOMIC_NUMBER_GET_AND_SET.getValue(), new AtomicLongGetAndSetHandler());
        registerHandler(ATOMIC_NUMBER_GET_AND_ADD.getValue(), new AtomicLongGetAndAddHandler());
        registerHandler(COUNT_DOWN_LATCH_AWAIT.getValue(), new CountDownLatchAwaitHandler());
        registerHandler(COUNT_DOWN_LATCH_COUNT_DOWN.getValue(), new CountDownLatchCountDownHandler());
        registerHandler(COUNT_DOWN_LATCH_GET_COUNT.getValue(), new CountDownLatchGetCountHandler());
        registerHandler(COUNT_DOWN_LATCH_GET_OWNER.getValue(), new CountDownLatchGetOwnerHandler());
        registerHandler(COUNT_DOWN_LATCH_SET_COUNT.getValue(), new CountDownLatchSetCountHandler());
        registerHandler(SEMAPHORE_ATTACH_DETACH_PERMITS.getValue(), new SemaphoreAttachDetachHandler());
        registerHandler(SEMAPHORE_CANCEL_ACQUIRE.getValue(), new SemaphoreCancelAcquireHandler());
        registerHandler(SEMAPHORE_DRAIN_PERMITS.getValue(), new SemaphoreDrainHandler());
        registerHandler(SEMAPHORE_GET_ATTACHED_PERMITS.getValue(), new SemaphoreGetAttachedHandler());
        registerHandler(SEMAPHORE_GET_AVAILABLE_PERMITS.getValue(), new SemaphoreGetAvailableHandler());
        registerHandler(SEMAPHORE_REDUCE_PERMITS.getValue(), new SemaphoreReduceHandler());
        registerHandler(SEMAPHORE_RELEASE.getValue(), new SemaphoreReleaseHandler());
        registerHandler(SEMAPHORE_TRY_ACQUIRE.getValue(), new SemaphoreTryAcquireHandler());
        registerHandler(LOCK_LOCK.getValue(), new LockOperationHandler());
        registerHandler(LOCK_UNLOCK.getValue(), new UnlockOperationHandler());
        registerHandler(LOCK_FORCE_UNLOCK.getValue(), new UnlockOperationHandler());
        registerHandler(LOCK_IS_LOCKED.getValue(), new IsLockedOperationHandler());
        node.connectionManager.addConnectionListener(this);
        this.THREAD_COUNT = node.getGroupProperties().EXECUTOR_CLIENT_THREAD_COUNT.getInteger();
        workers = new Worker[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            workers[i] = new Worker();
        }
        this.factory = node.factory;
    }

    void registerHandler(short operation, ClientOperationHandler handler) {
        clientOperationHandlers[operation] = handler;
    }

    public void handle(Protocol protocol) {
        checkFirstCall();
        ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
        CallContext callContext = clientEndpoint.getCallContext(0);
        CommandHandler handler = mapCommandHandlers.get(protocol.getCommand());
        if (handler == null) {
            handler = unknownOperationHandler;
        }
        System.out.println(protocol.getCommand() + "::: Handler is :" + handler);
        if (!clientEndpoint.isAuthenticated() && !Command.AUTH.value.equals(protocol.getCommand())) {
            checkAuth(protocol.getConnection());
            return;
        }
        ClientRequestHandler clientRequestHandler = new ClientProtocolRequestHandler(node, protocol, callContext,
                handler, clientEndpoint.getSubject(), protocol.getConnection());
        int hash = hash(callContext.getThreadId(), THREAD_COUNT);
        workers[hash].addWork(clientRequestHandler);
    }

    // always called by InThread
    public void handle(Packet packet) {
        checkFirstCall();
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        ClientOperationHandler handler = clientOperationHandlers[packet.operation.getValue()];
        if (handler == null) {
            handler = unknownOperationHandler;
        }
        if (!clientEndpoint.isAuthenticated() && packet.operation != CLIENT_AUTHENTICATE) {
            checkAuth(packet.conn);
            return;
        }
        ClientRequestHandler clientRequestHandler = new ClientPacketRequestHandler(node, packet, callContext,
                handler, clientEndpoint.getSubject(), packet.conn);
        clientEndpoint.addRequest(clientRequestHandler);
        if (packet.operation == CONCURRENT_MAP_UNLOCK) {
            node.executorManager.executeNow(clientRequestHandler);
        } else {
            int hash = hash(callContext.getThreadId(), THREAD_COUNT);
            workers[hash].addWork(clientRequestHandler);
        }
    }

    private void checkAuth(Connection conn) {
        logger.log(Level.SEVERE, "A Client " + conn + " must authenticate before any operation.");
        node.clientHandlerService.removeClientEndpoint(conn);
        if (conn != null)
            conn.close();
        return;
    }

    private void checkFirstCall() {
        if (firstCall) {
            String threadNamePrefix = node.getThreadPoolNamePrefix("client.service");
            for (int i = 0; i < THREAD_COUNT; i++) {
                Worker worker = workers[i];
                new Thread(node.threadGroup, worker, threadNamePrefix + i).start();
            }
            firstCall = false;
        }
    }

    public void shutdown() {
        mapClientEndpoints.clear();
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    public void restart() {
        for (List<ListenerManager.ListenerItem> listeners : node.listenerManager.namedListeners.values()) {
            for (ListenerManager.ListenerItem listener : listeners) {
                if (listener instanceof ClientListener) {
                    node.listenerManager.removeListener(listener.name, listener, listener.key);
                }
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
                    if (node.securityContext != null) {
                        try {
                            clientEndpoint.getLoginContext().logout();
                        } catch (LoginException e) {
                            logger.log(Level.WARNING, e.getMessage(), e);
                        }
                    }
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

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            long timeout = Long.valueOf(protocol.getArgs()[1]);
            Data item = new Data(protocol.getBuffers()[0].array());
            IQueue<Data> queue = node.factory.getQueue(name);
            boolean result = false;
            try {
                if (timeout == -1) {
                    queue.put(item);
                    result = true;
                } else if (timeout == 0) {
                    result = queue.offer(item);
                } else {
                    result = queue.offer(item, timeout, TimeUnit.MILLISECONDS);
                }
                return protocol.success(String.valueOf(result));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            long timeout = Long.valueOf(protocol.getArgs()[1]);
            Data item = new Data(protocol.getBuffers()[0].array());
            IQueue<Data> queue = node.factory.getQueue(name);
            Data result = null;
            try {
                if (timeout == -1) {
                    result = queue.take();
                } else if (timeout == 0) {
                    result = queue.poll();
                } else {
                    result = queue.poll(timeout, TimeUnit.MILLISECONDS);
                }
                return protocol.success(result == null ? null : ByteBuffer.wrap(result.buffer));
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

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data item = new Data(protocol.getBuffers()[0].array());
            IQueue<Data> queue = node.factory.getQueue(name);
            boolean removed = queue.remove(item);
            return protocol.success(String.valueOf(removed));
        }
    }

    private class QueuePeekHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            return (Data) queue.peek();
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data entry = (Data) node.factory.getQueue(name).peek();
            return protocol.success(ByteBuffer.wrap(entry.buffer));
        }
    }

    private class QueueSizeHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            return toData(queue.size());
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            return protocol.success(String.valueOf(node.factory.getQueue(name).size()));
        }
    }

    private class TopicPublishHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ITopic<Object> topic = (ITopic) factory.getOrCreateProxyByName(packet.name);
            topic.publish(packet.getKeyData());
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data data = new Data(protocol.getBuffers()[0].array());
            node.factory.getTopic(name).publish(data);
            return protocol.success();
        }

        @Override
        protected void sendResponse(SocketWritable request, Connection conn) {
        }
    }

    private class QueueRemainingCapacityHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            return toData(queue.remainingCapacity());
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            return protocol.success(String.valueOf(node.factory.getQueue(name).remainingCapacity()));
        }
    }

    private class QueueEntriesHandler extends ClientQueueOperationHandler {
        public Data processQueueOp(IQueue<Object> queue, Data key, Data value) {
            Object[] array = queue.toArray();
            Keys keys = new Keys();
            for (Object o : array) {
                keys.add(toData(o));
            }
            return toData(keys);
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data[] entries = node.factory.getQueue(name).toArray(new Data[0]);
            ByteBuffer[] buffers = new ByteBuffer[entries.length];
            for (int i = 0; i < entries.length; i++) {
                buffers[i] = ByteBuffer.wrap(entries[i].buffer);
            }
            return protocol.success(buffers);
        }
    }

    private class RemotelyProcessHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            node.clusterService.enqueuePacket(packet);
        }

        @Override
        protected void sendResponse(SocketWritable request, Connection conn) {
        }
    }

    private class DestroyHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Instance instance = (Instance) factory.getOrCreateProxyByName(packet.name);
            instance.destroy();
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String type = protocol.getArgs()[0];
            String name = protocol.getArgs()[1];
            if (InstanceType.MAP.toString().equalsIgnoreCase(name)) {
                node.factory.getMap(name).destroy();
            } else if (InstanceType.QUEUE.toString().equalsIgnoreCase(name)) {
                node.factory.getQueue(name).destroy();
            } else if (InstanceType.SET.toString().equalsIgnoreCase(name)) {
                node.factory.getSet(name).destroy();
            } else if (InstanceType.LIST.toString().equalsIgnoreCase(name)) {
                node.factory.getList(name).destroy();
            } else if (InstanceType.MULTIMAP.toString().equalsIgnoreCase(name)) {
                node.factory.getMultiMap(name).destroy();
            } else if (InstanceType.TOPIC.toString().equalsIgnoreCase(name)) {
                node.factory.getTopic(name).destroy();
            } else if (InstanceType.ATOMIC_NUMBER.toString().equalsIgnoreCase(name)) {
                node.factory.getAtomicNumber(name).destroy();
            } else if (InstanceType.ID_GENERATOR.toString().equalsIgnoreCase(name)) {
                node.factory.getIdGenerator(name).destroy();
            } else if (InstanceType.LOCK.toString().equalsIgnoreCase(name)) {
                node.factory.getLock(name).destroy();
            } else if (InstanceType.SEMAPHORE.toString().equalsIgnoreCase(name)) {
                node.factory.getSemaphore(name).destroy();
            } else if (InstanceType.COUNT_DOWN_LATCH.toString().equalsIgnoreCase(name)) {
                node.factory.getCountDownLatch(name).destroy();
            } else {
                return protocol.error(null, "unkonwn", "type");
            }
            return protocol.success();
        }
    }

    private class NewIdHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            IdGenerator idGen = (IdGenerator) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(idGen.newId()));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            return protocol.success(String.valueOf(node.factory.getIdGenerator(protocol.getArgs()[0]).newId()));
        }
    }

    private class MapPutMultiHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(multiMap.put(packet.getKeyData(), packet.getValueData())));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = new Data(protocol.getBuffers()[0].array());
            Data value = new Data(protocol.getBuffers()[1].array());
            return protocol.success(String.valueOf(node.factory.getMultiMap(name).put(key, value)));
        }
    }

    private class MapValueCountHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(multiMap.valueCount(packet.getKeyData())));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = new Data(protocol.getBuffers()[0].array());
            return protocol.success(String.valueOf(node.factory.getMultiMap(name).valueCount(key)));
        }
    }

    private class MapRemoveMultiHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
            if (packet.getValueData() == null || packet.getValueData().size() == 0) {
                MultiMapProxy mmProxy = (MultiMapProxy) multiMap;
                MProxy mapProxy = mmProxy.getMProxy();
                packet.setValue((Data) mapProxy.remove(packet.getKeyData()));
            } else {
                packet.setValue(toData(multiMap.remove(packet.getKeyData(), packet.getValueData())));
            }
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = new Data(protocol.getBuffers()[0].array());
            Data value = null;
            if (protocol.getBuffers().length > 1) {
                value = new Data(protocol.getBuffers()[1].array());
                return protocol.success(String.valueOf(node.factory.getMap(name).remove(key, value)));
            }
            return protocol.success(String.valueOf(node.factory.getMap(name).remove(key)));
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
                ExecutorService executorService = factory.getExecutorService(name);
                ClientDistributedTask cdt = (ClientDistributedTask) toObject(packet.getKeyData());
                if (cdt.getMember() != null && cdt.getMember() instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) cdt.getMember()).setHazelcastInstance(node.factory);
                }
                if (cdt.getMembers() != null) {
                    Set<Member> set = cdt.getMembers();
                    for (Member m : set)
                        if (m instanceof HazelcastInstanceAware)
                            ((HazelcastInstanceAware) m).setHazelcastInstance(node.factory);
                }
                final ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
                final Callable callable = node.securityContext == null
                        ? cdt.getCallable()
                        : node.securityContext.createSecureCallable(clientEndpoint.getSubject(), cdt.getCallable());
                final DistributedTask task;
                if (cdt.getKey() != null) {
                    task = new DistributedTask(callable, cdt.getKey());
                } else if (cdt.getMember() != null) {
                    task = new DistributedTask(callable, cdt.getMember());
                } else if (cdt.getMembers() != null) {
                    task = new MultiTask(callable, cdt.getMembers());
                } else {
                    task = new DistributedTask(callable);
                }
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
                        packet.lockAddress = null;
                        packet.responseType = RESPONSE_SUCCESS;
                        packet.operation = RESPONSE;
                        sendResponse(packet, packet.conn);
                    }
                });
                executorService.execute(task);
            } catch (RuntimeException e) {
                logger.log(Level.WARNING,
                        "exception during handling " + packet.operation + ": " + e.getMessage(), e);
                packet.clearForResponse();
                packet.setValue(toData(e));
                packet.lockAddress = null;
                packet.responseType = RESPONSE_SUCCESS;
                packet.operation = RESPONSE;
                sendResponse(packet, packet.conn);
            }
        }
    }

    private class GetInstancesHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Collection<Instance> instances = factory.getInstances();
            Keys keys = new Keys();
            for (Instance instance : instances) {
                Object id = instance.getId();
                if (id instanceof FactoryImpl.ProxyKey) {
                    Object key = ((FactoryImpl.ProxyKey) id).getKey();
                    if (key instanceof Instance)
                        id = key.toString();
                }
                String idStr = id.toString();
                if (!idStr.startsWith(Prefix.MAP_OF_LIST) && !idStr.startsWith(Prefix.MAP_FOR_QUEUE)) {
                    keys.add(toData(id));
                }
            }
            packet.setValue(toData(keys));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            Collection<Instance> collection = factory.getInstances();
            String[] args = new String[2 * collection.size()];
            int i = 0;
            for (Instance instance : collection) {
                args[i++] = instance.getInstanceType().toString();
                args[i++] = instance.getId().toString();
            }
            return protocol.success(args);
        }
    }

    private class GetMembersHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Cluster cluster = factory.getCluster();
            Set<Member> members = cluster.getMembers();
            Set<Data> setData = new LinkedHashSet<Data>();
            if (members != null) {
                for (Iterator<Member> iterator = members.iterator(); iterator.hasNext(); ) {
                    Member member = iterator.next();
                    setData.add(toData(member));
                }
                Keys keys = new Keys(setData);
                packet.setValue(toData(keys));
            }
        }

        public Protocol processCall(Node node, Protocol protocol) {
            Collection<Member> collection = factory.getCluster().getMembers();
            String[] args = new String[collection.size()];
            int i = 0;
            for (Member member : collection) {
                args[i++] = member.getInetSocketAddress().toString();
            }
            return protocol.success(args);
        }
    }

    private class GetPartitionsHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            PartitionService partitionService = factory.getPartitionService();
            if (packet.getKeyData() != null && packet.getKeyData().size() > 0) {
                Object key = toObject(packet.getKeyData());
                Partition partition = partitionService.getPartition(key);
                Data value = toData(new PartitionImpl(partition.getPartitionId(), (MemberImpl) partition.getOwner()));
                packet.setValue(value);
            } else {
                Set<Partition> partitions = partitionService.getPartitions();
                Set<Data> setData = new LinkedHashSet<Data>();
                for (Iterator<Partition> iterator = partitions.iterator(); iterator.hasNext(); ) {
                    Partition partition = iterator.next();
                    setData.add(toData(new PartitionImpl(partition.getPartitionId(), (MemberImpl) partition.getOwner())));
                }
                Keys keys = new Keys(setData);
                packet.setValue(toData(keys));
            }
        }

        public Protocol processCall(Node node, Protocol protocol) {
            PartitionService partitionService = factory.getPartitionService();
            List<String> args = new ArrayList<String>();
            if (protocol.getBuffers().length > 0) {
                Partition partition = partitionService.getPartition(new Data(protocol.getBuffers()[0].array()));
                args.add(String.valueOf(partition.getPartitionId()));
                args.add(partition.getOwner().getInetSocketAddress().toString());
            } else {
                Set<Partition> set = partitionService.getPartitions();
                for (Partition partition : set) {
                    args.add(String.valueOf(partition.getPartitionId()));
                    args.add(partition.getOwner().getInetSocketAddress().toString());
                }
            }
            return protocol.success(args.toArray(new String[0]));
        }
    }

    abstract private class AtomicLongClientHandler extends ClientOperationHandler {
        abstract Object processCall(AtomicNumberProxy atomicLongProxy, Long value, Long expected);

        public void processCall(Node node, Packet packet) {
            final AtomicNumberProxy atomicLong = (AtomicNumberProxy) factory.getOrCreateProxyByName(packet.name);
            final Long value = (Long) toObject(packet.getValueData());
            final Long expectedValue = (Long) toObject(packet.getKeyData());
            packet.setValue(toData(processCall(atomicLong, value, expectedValue)));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            long value = Long.valueOf(protocol.getArgs()[1]);
            long expectedValue = 0;
            if (protocol.getArgs().length > 2) {
                expectedValue = Long.valueOf(protocol.getArgs()[2]);
            }
            Object response = processCall((AtomicNumberProxy) node.factory.getAtomicNumber(name), value, expectedValue);
            return protocol.success(response.toString());
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
        abstract Object processCall(CountDownLatchProxy cdlProxy, Integer value, Address address) throws Exception;

        public void processCall(Node node, Packet packet) {
            final String name = packet.name.substring(Prefix.COUNT_DOWN_LATCH.length());
            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) factory.getCountDownLatch(name);
            final Integer value = (Integer) toObject(packet.getValueData());
            Object result = null;
            try {
                result = processCall(cdlProxy, value, packet.conn.getEndPoint());
                if (result != null) packet.setValue(toData(result));
            } catch (Exception e) {
                packet.setValue(toData(new ClientServiceException(e)));
            }
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Integer value = protocol.getArgs().length > 1 ? Integer.valueOf(protocol.getArgs()[1]) : 0;
            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) node.factory.getCountDownLatch(name);
            Object response = null;
            try {
                response = processCall(cdlProxy, value, protocol.getConnection().getEndPoint());
            } catch (Exception e) {
                return protocol.error(null, e.getMessage());
            }
            if (response == null) return protocol.success();
            else return protocol.success(response.toString());
        }
    }

    private class CountDownLatchAwaitHandler extends CountDownLatchClientHandler {
        Object processCall(CountDownLatchProxy cdlProxy, Integer value, Address address) throws Exception{
            return cdlProxy.await(value, TimeUnit.MILLISECONDS);
        }

        public void processCall(Node node, Packet packet) {
            final String name = packet.name.substring(Prefix.COUNT_DOWN_LATCH.length());
            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) factory.getCountDownLatch(name);
            final Integer value = (Integer) toObject(packet.getValueData());
            try {
                packet.setValue(toData(cdlProxy.await(packet.timeout, TimeUnit.MILLISECONDS)));
            } catch (Throwable e) {
                packet.setValue(toData(new ClientServiceException(e)));
            }
        }
    }

    private class CountDownLatchCountDownHandler extends CountDownLatchClientHandler {
        Object processCall(CountDownLatchProxy cdlProxy, Integer value, Address address) {
            cdlProxy.countDown();
            return null;
        }
    }

    private class CountDownLatchGetCountHandler extends CountDownLatchClientHandler {
        Object processCall(CountDownLatchProxy cdlProxy, Integer value, Address address) {
            return cdlProxy.getCount();
        }
    }

    private class CountDownLatchGetOwnerHandler extends CountDownLatchClientHandler {
        Object processCall(CountDownLatchProxy cdlProxy, Integer value, Address address) {
            return cdlProxy.getOwner();
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Integer value = protocol.getArgs().length > 1 ? Integer.valueOf(protocol.getArgs()[1]) : 0;
            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) node.factory.getCountDownLatch(name);
            Member m = cdlProxy.getOwner();
            return protocol.success(m.getInetSocketAddress().toString());
        }
    }

    private class CountDownLatchSetCountHandler extends CountDownLatchClientHandler {
        Object processCall(CountDownLatchProxy cdlProxy, Integer count, Address address) {
            boolean isSet = cdlProxy.setCount(count, address);
            return isSet;
        }

//        @Override
//        public void processCall(Node node, Packet packet) {
//            final String name = packet.name.substring(Prefix.COUNT_DOWN_LATCH.length());
//            final CountDownLatchProxy cdlProxy = (CountDownLatchProxy) factory.getCountDownLatch(name);
//            final Integer value = (Integer) toObject(packet.getValueData());
//            try {
//                Address ownerAddress = packet.conn.getEndPoint();
//                packet.setValue(toData(cdlProxy.setCount(value, ownerAddress)));
//            } catch (Throwable e) {
//                packet.setValue(toData(new ClientServiceException(e)));
//            }
//        }
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
            final SemaphoreProxy semaphoreProxy = (SemaphoreProxy) factory.getSemaphore(packet.name);
            final Integer value = (Integer) toObject(packet.getValueData());
            final boolean flag = (Boolean) toObject(packet.getKeyData());
            processCall(packet, semaphoreProxy, value, flag);
        }
    }

    private class SemaphoreAttachDetachHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean attach) {
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
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag) {
            packet.setValue(toData(semaphoreProxy.attachedPermits()));
        }
    }

    private class SemaphoreGetAvailableHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag) {
            packet.setValue(toData(semaphoreProxy.availablePermits()));
        }
    }

    private class SemaphoreDrainHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer value, boolean flag) {
            packet.setValue(toData(semaphoreProxy.drainPermits()));
        }
    }

    private class SemaphoreReduceHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean flag) {
            semaphoreProxy.reducePermits(permits);
        }
    }

    private class SemaphoreReleaseHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean detach) {
            if (detach) {
                semaphoreProxy.releaseDetach(permits);
                getClientEndpoint(packet.conn).attachDetachPermits(packet.name, -permits);
            } else {
                semaphoreProxy.release(permits);
            }
        }
    }

    private class SemaphoreTryAcquireHandler extends SemaphoreClientOperationHandler {
        void processCall(Packet packet, SemaphoreProxy semaphoreProxy, Integer permits, boolean attach) {
            try {
                boolean acquired;
                if (attach) {
                    acquired = semaphoreProxy.tryAcquireAttach(permits, packet.timeout, TimeUnit.MILLISECONDS);
                    if (acquired) {
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
            Cluster cluster = factory.getCluster();
            long clusterTime = cluster.getClusterTime();
            packet.setValue(toData(clusterTime));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            return protocol.success(String.valueOf(node.factory.getCluster().getClusterTime()));
        }
    }

    class ClientAuthenticateHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            final Credentials credentials = (Credentials) toObject(packet.getValueData());
            boolean authenticated = doAuthenticate(node, credentials, packet.conn);
            packet.clearForResponse();
            packet.setValue(toData(authenticated));
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            Credentials credentials;
            String[] args = protocol.getArgs();
            if (node.securityContext == null) {
                if (args.length < 2) {
                    throw new RuntimeException("Should provide both username and password");
                }
                credentials = new UsernamePasswordCredentials(args[0], args[1]);
            } else {
                credentials = (Credentials) toObject(new Data(protocol.getBuffers()[0].array()));
            }
            boolean authenticated = doAuthenticate(node, credentials, protocol.getConnection());
            return authenticated ? protocol.success() : protocol.error(null);
        }

        private boolean doAuthenticate(Node node, Credentials credentials, Connection conn) {
            boolean authenticated;
            if (credentials == null) {
                authenticated = false;
                logger.log(Level.SEVERE, "Could not retrieve Credentials object!");
            } else if (node.securityContext != null) {
                final Socket endpointSocket = conn.getSocketChannelWrapper().socket();
                // TODO: check!!!
                credentials.setEndpoint(endpointSocket.getInetAddress().getHostAddress());
                try {
                    LoginContext lc = node.securityContext.createClientLoginContext(credentials);
                    lc.login();
                    getClientEndpoint(conn).setLoginContext(lc);
                    authenticated = true;
                } catch (LoginException e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                    authenticated = false;
                }
            } else {
                if (credentials instanceof UsernamePasswordCredentials) {
                    final UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
                    final String nodeGroupName = factory.getConfig().getGroupConfig().getName();
                    final String nodeGroupPassword = factory.getConfig().getGroupConfig().getPassword();
                    authenticated = (nodeGroupName.equals(usernamePasswordCredentials.getUsername())
                            && nodeGroupPassword.equals(usernamePasswordCredentials.getPassword()));
                } else {
                    authenticated = false;
                    logger.log(Level.SEVERE, "Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster group-name" +
                            " and group-password should be used for authentication!\n" +
                            "Current credentials type is: " + credentials.getClass().getName());
                }
            }
            logger.log((authenticated ? Level.INFO : Level.WARNING), "received auth from " + conn
                    + ", " + (authenticated ?
                    "successfully authenticated" : "authentication failed"));
            if (!authenticated) {
                node.clientHandlerService.removeClientEndpoint(conn);
            } else {
                ClientEndpoint clientEndpoint = node.clientHandlerService.getClientEndpoint(conn);
                clientEndpoint.authenticated();
                Bind bind = new Bind(new Address(packet.conn.getSocketChannelWrapper().socket().getInetAddress(), packet.conn.getSocketChannelWrapper().socket().getPort()));
                bind.setConnection(packet.conn);
                bind.setNode(node);
//                System.out.println("bind = " + bind);
                node.clusterService.enqueueAndWait(bind);
            }
            return authenticated;
        }
    }

    private class ClientAddInstanceListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ClientEndpoint endPoint = getClientEndpoint(packet.conn);
            factory.addInstanceListener(endPoint);
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
            } finally {
                return null;
            }
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            int size = protocol.getBuffers() != null && protocol.getBuffers().length > 0 ? protocol.getBuffers().length : 0;
            Map map = new HashMap();
            for (int i = 0; i < size; i++) {
                map.put(new Data(protocol.getBuffers()[i].array()), new Data(protocol.getBuffers()[++i].array()));
            }
            node.factory.getMap(name).putAll(map);
            return protocol.success();
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
                System.out.println("Now doing put key " + new String(key.buffer));
                Data result = (Data) map.put(key, v);
                System.out.println(ThreadContext.get().getCallContext().getThreadId() + " Result of put = " + result);
                return result;
            } else {
                return (Data) map.put(key, v, ttl, TimeUnit.MILLISECONDS);
            }
        }
    }

    private class MapPutTransientHandler extends ClientMapOperationHandlerWithTTL {
        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            map.putTransient(key, v, ttl, TimeUnit.MILLISECONDS);
            return null;
        }
    }

    private class MapSetHandler extends ClientMapOperationHandlerWithTTL {
        @Override
        protected Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            map.set(key, v, ttl, TimeUnit.MILLISECONDS);
            return null;
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

        public Protocol processCall(Node node, Protocol protocol) {
            String[] args = protocol.getArgs();
            final IMap<Object, Object> map = (IMap) factory.getMap(args[0]);
            final long ttl = (args.length > 1) ? Long.valueOf(args[1]) : 0;
            Data value = processMapOp(map, new Data(protocol.getBuffers()[0].array()), new Data(protocol.getBuffers()[1].array()), ttl);
            Boolean bValue = (Boolean) toObject(value);
            return protocol.success(String.valueOf(bValue));
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

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String[] args = protocol.getArgs();
            String name = args[0];
            final long ttl = Integer.parseInt(args[1]);
            final IMap<Object, Object> map = (IMap) factory.getMap(name);
            Data value;
            try {
                System.out.println("key is " + new String(protocol.getBuffers()[0].array()));

                value = (Data) map.tryRemove(new Data(protocol.getBuffers()[0].array()), ttl, TimeUnit.MILLISECONDS);
                System.out.println("VALUE is " + value);
            } catch (TimeoutException e) {
                return protocol.success("timeout");
            }
            return protocol.success((value == null) ? null : ByteBuffer.wrap(value.buffer));
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

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            long ttl = protocol.getArgs().length > 1 ? Long.valueOf(protocol.getArgs()[1]) : 0;
            Data key = new Data(protocol.getBuffers()[0].array());
            Data value = new Data(protocol.getBuffers()[1].array());
            Data oldValue = processMapOp(node.factory.getMap(name), key, value, ttl);

            return protocol.success(oldValue == null? null: ByteBuffer.wrap(oldValue.buffer));
        }

        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
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

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            int size = protocol.getBuffers() != null && protocol.getBuffers().length > 0 ? protocol.getBuffers().length : 0;
            Set<Object> set = new HashSet<Object>();
            for (int i = 0; i < size; i++) {
                set.add(new Data(protocol.getBuffers()[i].array()));
            }
            Map<Object, Object> map = node.factory.getMap(name).getAll(set);
            ByteBuffer[] buffers = new ByteBuffer[size];
            int i = 0;
            for (Object k : set) {
                Object v = map.get(k);
                if (v == null) {
                    buffers[i++] = ByteBuffer.wrap(new byte[0]);
                } else {
                    buffers[i++] = ByteBuffer.wrap(((Data) v).buffer);
                }
            }
            return protocol.success(buffers);
        }
    }

    private class GetMapEntryHandler extends ClientMapOperationHandler {

        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.getMapEntry(key));
        }

        public Protocol processMapOp(Protocol protocol, IMap<Object, Object> map, Data key, Data value) {
            MapEntry<Object, Object> mapEntry = map.getMapEntry(key);
            System.out.println("mapEntry = " + mapEntry);
            if (mapEntry == null)
                return protocol.success();
            else
                return protocol.success(ByteBuffer.wrap(((Data) mapEntry.getValue()).buffer),
                        String.valueOf(mapEntry.getCost()), String.valueOf(mapEntry.getCreationTime()),
                        String.valueOf(mapEntry.getExpirationTime()), String.valueOf(mapEntry.getHits()),
                        String.valueOf(mapEntry.getLastAccessTime()), String.valueOf(mapEntry.getLastStoredTime()),
                        String.valueOf(mapEntry.getLastUpdateTime()), String.valueOf(mapEntry.getVersion()), String.valueOf(mapEntry.isValid()));
        }
    }

    private class MapGetHandler extends ClientOperationHandler {

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            byte[] key = protocol.getBuffers()[0].array();
            Data value = (Data) node.factory.getMap(name).get(new Data(key));
            return protocol.success(value == null ? null : ByteBuffer.wrap(value.buffer));
        }

        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            Data key = packet.getKeyData();
            if (instanceType == InstanceType.MAP) {
                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
                packet.setKey(null);
                packet.setValue((Data) map.get(key));
            } else if (instanceType == InstanceType.MULTIMAP) {
                MultiMapProxy multiMap = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
                MProxy mapProxy = multiMap.getMProxy();
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

        @Override
        public Protocol processMapOp(Protocol protocol, IMap<Object, Object> map, Data key, Data value) {
            Data result = processMapOp(map, key, value);
            boolean evicted = (Boolean) toObject(result);
            return protocol.success(String.valueOf(evicted));
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
            Keys keys = (Keys) toObject(value);
            Iterator it = keys.getKeys().iterator();
            Data expected = (Data) it.next();
            Data newValue = (Data) it.next();
            boolean replaced = map.replace(key, expected, newValue);
            return toData(replaced);
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = null;
            Data oldValue = null;
            Data newValue = null;
            if (protocol.getBuffers() != null && protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
                if (protocol.getBuffers().length > 1) {
                    oldValue = new Data(protocol.getBuffers()[1].array());
                    if (protocol.getBuffers().length > 2) {
                        newValue = new Data(protocol.getBuffers()[2].array());
                    }
                    else{
                        return protocol.error(null, "New value is missing");
                    }
                }
                else{
                    return protocol.error(null, "old value is missing");
                }
            }
            else{
                return protocol.error(null, "key is missing");
            }
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            boolean replaced = map.replace(key, oldValue, newValue );
            return protocol.success(String.valueOf(replaced));
        }
    }

    private class MapContainsHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(map.containsKey(packet.getKeyData())));
            } else if (instanceType.equals(InstanceType.LIST) || instanceType.equals(InstanceType.SET)) {
                Collection<Object> collection = (Collection) factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(collection.contains(packet.getKeyData())));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(multiMap.containsKey(packet.getKeyData())));
            }
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = new Data(protocol.getBuffers()[0].array());
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            boolean contains = map.containsKey(key);
            return protocol.success(String.valueOf(contains));
        }
    }

    private class MapContainsValueHandler extends ClientOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.containsValue(value));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data value = new Data(protocol.getBuffers()[0].array());
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            boolean contains = map.containsValue(value);
            return protocol.success(String.valueOf(contains));

        }

        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
                packet.setValue(toData(map.containsValue(packet.getValueData())));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
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
            IList<Object> list = (IList) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(list.size()));
        }

        @Override
        public void doMapOp(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(map.size()));
        }

        @Override
        public void doSetOp(Node node, Packet packet) {
            ISet<Object> set = (ISet) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(set.size()));
        }

        @Override
        public void doMultiMapOp(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
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
        public Protocol processCall(Node node, Protocol protocol) {
            System.out.println("Locking from " + ThreadContext.get().getCallContext().getThreadId());
            String name = protocol.getArgs()[0];
            long timeout = -1;
            if(protocol.getCommand().equals(Command.MTRYLOCK)){
                if(protocol.getArgs().length > 0)
                    timeout = Long.valueOf(protocol.getArgs()[1]);
                else
                    timeout = 0;
            }
            boolean locked = true;
            Data key = null;
            if (protocol.getBuffers() != null && protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
            }
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            if (key == null) {
                map.lockMap(timeout, TimeUnit.MILLISECONDS);
            } else {
                if (timeout == -1) {
                    map.lock(key);
                } else if (timeout == 0) {
                    locked = map.tryLock(key);
                } else {
                    locked = map.tryLock(key, timeout, TimeUnit.MILLISECONDS);
                }
            }
            return protocol.success(String.valueOf(locked));
        }

        @Override
        public void processCall(Node node, Packet packet) {
            Instance.InstanceType type = getInstanceType(packet.name);
            long timeout = packet.timeout;
            Data value = null;
            IMap<Object, Object> map = null;
            if (type == Instance.InstanceType.MAP) {
                map = (IMap) factory.getOrCreateProxyByName(packet.name);
            } else {
                MultiMapProxy multiMapProxy = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
                map = multiMapProxy.getMProxy();
            }
            if (timeout == -1) {
                map.lock(packet.getKeyData());
                value = null;
            } else if (timeout == 0) {
                value = toData(map.tryLock(packet.getKeyData()));
            } else {
                TimeUnit timeUnit = (TimeUnit) toObject(packet.getValueData());
                timeUnit = timeUnit == null ? TimeUnit.MILLISECONDS : timeUnit;
                value = toData(map.tryLock(packet.getKeyData(), timeout, timeUnit));
            }
            packet.setValue(value);
        }
    }

    private class MapIsKeyLockedHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            throw new RuntimeException("Shouldn't invoke this method");
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = null;
            if (protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
            } else {
                protocol.error(null, "key", "is", "missing");
            }
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            return protocol.success(String.valueOf(map.isLocked(key)));
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = null;
            map = (IMap) factory.getOrCreateProxyByName(packet.name);
            packet.setValue(toData(map.isLocked(packet.getKeyData())));
        }
    }

    private class MapForceUnlockHandler extends MapUnlockHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.forceUnlock(key);
            return null;
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
                map = (IMap) factory.getOrCreateProxyByName(packet.name);
            } else {
                MultiMapProxy multiMapProxy = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
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
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            long timeout = Long.valueOf(protocol.getArgs()[1]);
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            boolean islocked = map.lockMap(timeout, TimeUnit.MILLISECONDS);
            return protocol.success(String.valueOf(islocked));
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
            long timeout = packet.timeout;
            TimeUnit timeUnit = (TimeUnit) toObject(packet.getValueData());
            timeUnit = (timeUnit == null) ? TimeUnit.MILLISECONDS : timeUnit;
            Data value = toData(map.lockMap(timeout, timeUnit));
            packet.setValue(value);
        }
    }

    private class MapUnlockMapHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.unlockMap();
            return null;
        }

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            processMapOp(map, null, null);
            return protocol.success();
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private class LockOperationHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            final Object key = toObject(packet.getKeyData());
            final ILock lock = factory.getLock(key);
            final long timeout = packet.timeout;
            Data value = null;
            if (timeout == -1) {
                lock.lock();
            } else if (timeout == 0) {
                value = toData(lock.tryLock());
            } else {
                try {
                    value = toData(lock.tryLock(timeout, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    logger.log(Level.FINEST, "Lock interrupted!");
                    value = toData(Boolean.FALSE);
                }
            }
            packet.clearForResponse();
            packet.setValue(value);
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            ILock lock = node.factory.getLock(name);
            try{
                if(protocol.getCommand().equals(Command.LOCK_TRYLOCK.value)){
                    if(protocol.getArgs().length > 1){
                        return protocol.success(String.valueOf(lock.tryLock(Long.valueOf(protocol.getArgs()[1]),
                                TimeUnit.MILLISECONDS)));
                    } else{
                        return protocol.success(String.valueOf(lock.tryLock()));
                    }
                } else{
                    lock.unlock();
                    return protocol.success();
                }
            }catch (InterruptedException e){
                logger.log(Level.FINEST, "Lock interrupted!");
                return protocol.success("interrupted");
            }
        }
    }

    private class IsLockedOperationHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            final Object key = toObject(packet.getKeyData());
            final ILock lock = factory.getLock(key);
            Data value = null;
            value = toData(lock.isLocked());
            packet.clearForResponse();
            packet.setValue(value);
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            ILock lock = node.factory.getLock(name);
            return protocol.success(String.valueOf(lock.isLocked()));
        }
    }

    private class UnlockOperationHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            final Object key = toObject(packet.getKeyData());
            final ILock lock = factory.getLock(key);
            if (packet.operation == LOCK_UNLOCK) {
                lock.unlock();
            } else if (packet.operation == LOCK_FORCE_UNLOCK) {
                lock.forceUnlock();
            }
            packet.clearForResponse();
            packet.setValue(null);
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            ILock lock = node.factory.getLock(name);
            if(protocol.getCommand().equals(Command.LOCK_UNLOCK.value)){
                lock.unlock();
            } else if (protocol.getCommand().equals(Command.LOCK_FORCE_UNLOCK.value)){
                lock.forceUnlock();
            }
            return protocol.success();
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

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String type = protocol.getArgs()[0];
            String name = protocol.getArgs()[1];
            if ("map".equals(type)) {
                Entries entries = (Entries) node.factory.getMap(name).entrySet();
                Collection<Map.Entry> colEntries = entries.getKeyValues();
                ByteBuffer[] buffers = new ByteBuffer[colEntries.size() * 2];
                int i = 0;
                System.out.println(colEntries.size());
                for (Object obj : colEntries) {
                    KeyValue entry = (KeyValue) obj;
                    Data key = entry.getKeyData();
                    Data value = entry.getValueData();
                    buffers[i++] = key == null ? ByteBuffer.allocate(0) : ByteBuffer.wrap(entry.getKeyData().buffer);
                    buffers[i++] = value == null ? ByteBuffer.allocate(0) : ByteBuffer.wrap(entry.getValueData().buffer);
                }
                return protocol.success(buffers);
            }
            return protocol;
        }

        public Data getMapKeys(final IMap<Object, Object> map, final Data key, final Data value) {
            Entries entries = null;
            if (value == null) {
                entries = (Entries) map.entrySet();
            } else {
                final Predicate p = (Predicate) toObject(value);
                entries = (Entries) map.entrySet(p);
            }
            final Collection<Map.Entry> colEntries = entries.getKeyValues();
            final Keys keys = new Keys(new ArrayList<Data>(colEntries.size() << 1));
            for (final Object obj : colEntries) {
                final KeyValue entry = (KeyValue) obj;
                keys.add(toData(entry));
            }
            return toData(keys);
        }

        @Override
        public void doListOp(final Node node, final Packet packet) {
            IMap mapProxy = (IMap) factory.getOrCreateProxyByName(Prefix.MAP + Prefix.QUEUE + packet.name);
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData()));
        }

        @Override
        public void doMapOp(final Node node, final Packet packet) {
            packet.setValue(getMapKeys((IMap) factory.getOrCreateProxyByName(packet.name), packet.getKeyData(), packet.getValueData()));
        }

        @Override
        public void doSetOp(final Node node, final Packet packet) {
            final SetProxy collectionProxy = (SetProxy) factory.getOrCreateProxyByName(packet.name);
            final MProxy mapProxy = collectionProxy.getMProxy();
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData()));
        }

        @Override
        public void doMultiMapOp(final Node node, final Packet packet) {
            final MultiMapProxy multiMap = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
            final MProxy mapProxy = multiMap.getMProxy();
            final Data value = getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }

        @Override
        public void doQueueOp(final Node node, final Packet packet) {
            final IQueue queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
        }
    }

    private class MapIterateKeysHandler extends ClientCollectionOperationHandler {
        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String type = protocol.getArgs()[0];
            String name = protocol.getArgs()[1];
            if ("map".equals(type)) {
                Entries entries = (Entries) node.factory.getMap(name).keySet();
                Collection<Map.Entry> colEntries = entries.getKeyValues();
                ByteBuffer[] buffers = new ByteBuffer[colEntries.size()];
                int i = 0;
                for (Object obj : colEntries) {
                    KeyValue entry = (KeyValue) obj;
                    buffers[i++] = ByteBuffer.wrap(entry.getKeyData().buffer);
                }
                return protocol.success(buffers);
            }
            return protocol;
        }

        public Data getMapKeys(IMap<Object, Object> map, Data key, Data value, Collection<Data> collection) {
            Entries entries = null;
            if (value == null) {
                entries = (Entries) map.keySet();
            } else {
                Predicate p = (Predicate) toObject(value);
                entries = (Entries) map.keySet(p);
            }
            Collection<Map.Entry> colEntries = entries.getKeyValues();
            Keys keys = new Keys(collection);
            for (Object obj : colEntries) {
                KeyValue entry = (KeyValue) obj;
                keys.add(entry.getKeyData());
            }
            return toData(keys);
        }

        @Override
        public void doListOp(Node node, Packet packet) {
            IMap mapProxy = (IMap) factory.getOrCreateProxyByName(Prefix.MAP + Prefix.QUEUE + packet.name);
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new ArrayList<Data>()));
        }

        @Override
        public void doMapOp(Node node, Packet packet) {
            packet.setValue(getMapKeys((IMap) factory.getOrCreateProxyByName(packet.name), packet.getKeyData(), packet.getValueData(), new HashSet<Data>()));
        }

        @Override
        public void doSetOp(Node node, Packet packet) {
            SetProxy collectionProxy = (SetProxy) factory.getOrCreateProxyByName(packet.name);
            MProxy mapProxy = collectionProxy.getMProxy();
            packet.setValue(getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new HashSet<Data>()));
        }

        public void doMultiMapOp(Node node, Packet packet) {
            MultiMapProxy multiMap = (MultiMapProxy) factory.getOrCreateProxyByName(packet.name);
            MProxy mapProxy = multiMap.getMProxy();
            Data value = getMapKeys(mapProxy, packet.getKeyData(), packet.getValueData(), new HashSet<Data>());
            packet.clearForResponse();
            packet.setValue(value);
        }

        public void doQueueOp(Node node, Packet packet) {
            IQueue queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
        }
    }

    private class MapAddListenerHandler extends ClientOperationHandler {

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            final ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
            String[] args = protocol.getArgs();
            String name = args[0];
            boolean includeValue = Boolean.valueOf(args[1]);
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            Data key = null;
            if (protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
            }
            clientEndpoint.addThisAsListener(map, key, includeValue);
            return protocol.success();
        }

        public void processCall(Node node, Packet packet) {
        }
    }

    private class MapRemoveListenerHandler extends ClientOperationHandler {

        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = null;
            if (protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
            }
            ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
            IMap map = factory.getMap(name);
            clientEndpoint.removeThisListener(map, key);
            return protocol.success();
        }

        public void processCall(Node node, Packet packet) {
        }
    }

    private class QueueAddListenerHandler extends ClientOperationHandler {

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            final ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
            String name = protocol.getArgs()[0];
            boolean includeValue = Boolean.valueOf(protocol.getArgs()[1]);
            IQueue<Object> queue = (IQueue) factory.getQueue(name);
            ItemListener itemListener = new ClientItemListener(clientEndpoint, name);
            queue.addItemListener(itemListener, includeValue);
            clientEndpoint.queueItemListeners.put(queue, itemListener);
            return protocol.success();
        }

        public void processCall(Node node, Packet packet) {
        }
    }

    private class QueueRemoveListenerHandler extends ClientOperationHandler {

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            final ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
            String name = protocol.getArgs()[0];
            IQueue<Object> queue = (IQueue) factory.getQueue(name);
            ItemListener itemListener = clientEndpoint.queueItemListeners.remove(name);
            queue.removeItemListener(itemListener);
            return protocol.success();
        }

        public void processCall(Node node, Packet packet) {
        }
    }

    private class AddListenerHandler extends ClientOperationHandler {

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            final ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
            String[] args = protocol.getArgs();
            String type = args[0];
            String name = args[1];
            boolean includeValue = Boolean.valueOf(args[2]);
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            Data key = null;
            if ("map".equals(type)) {
                if (protocol.getBuffers() != null && protocol.getBuffers().length > 0) {
                    key = new Data(protocol.getBuffers()[0].array());
                }
                System.out.println(includeValue + " Key is " + key);
                clientEndpoint.addThisAsListener(map, key, includeValue);
            }
            return protocol.success();
        }

        public void processCall(Node node, final Packet packet) {
            final ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
            boolean includeValue = (int) packet.longValue == 1;
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.addThisAsListener(map, packet.getKeyData(), includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.MULTIMAP)) {
                MultiMap<Object, Object> multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.addThisAsListener(multiMap, packet.getKeyData(), includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.LIST)) {
                ListProxyImpl listProxy = (ListProxyImpl) factory.getOrCreateProxyByName(packet.name);
                IMap map = (IMap) factory.getOrCreateProxyByName(Prefix.MAP + (String) listProxy.getId());
                clientEndpoint.addThisAsListener(map, null, includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.SET)) {
                SetProxy collectionProxy = (SetProxy) factory.getOrCreateProxyByName(packet.name);
                IMap map = collectionProxy.getMProxy();
                clientEndpoint.addThisAsListener(map, null, includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.QUEUE)) {
                IQueue<Object> queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
                final String packetName = packet.name;
                ItemListener itemListener = new ClientItemListener(clientEndpoint, packetName);
                queue.addItemListener(itemListener, includeValue);
                clientEndpoint.queueItemListeners.put(queue, itemListener);
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic<Object> topic = (ITopic) factory.getOrCreateProxyByName(packet.name);
                final String packetName = packet.name;
                MessageListener messageListener = new ClientMessageListener(clientEndpoint, packetName);
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

        public void itemAdded(ItemEvent itemEvent) {
            Packet p = new Packet();
            DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) itemEvent;
            p.set(name, ClusterOperation.EVENT, dataAwareItemEvent.getItemData(), true);
            clientEndpoint.sendPacket(p);
        }

        public void itemRemoved(ItemEvent itemEvent) {
            Packet p = new Packet();
            DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) itemEvent;
            p.set(name, ClusterOperation.EVENT, dataAwareItemEvent.getItemData(), false);
            clientEndpoint.sendPacket(p);
        }
    }

    public class ClientMessageListener implements MessageListener, ClientListener {
        final ClientEndpoint clientEndpoint;
        final String name;

        ClientMessageListener(ClientEndpoint clientEndpoint, String name) {
            this.clientEndpoint = clientEndpoint;
            this.name = name;
        }

        public void onMessage(Message msg) {
            Packet p = new Packet();
            DataMessage dataMessage = (DataMessage) msg;
            p.set(name, ClusterOperation.EVENT, dataMessage.getMessageData(), null);
            clientEndpoint.sendPacket(p);
        }
    }

    private class RemoveListenerHandler extends ClientOperationHandler {
        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String type = protocol.getArgs()[0];
            String name = protocol.getArgs()[1];
            Data key = null;
            if (protocol.getBuffers() != null && protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
            }
            ClientEndpoint clientEndpoint = getClientEndpoint(protocol.getConnection());
            if ("map".equals(type)) {
                IMap map = factory.getMap(name);
                clientEndpoint.removeThisListener(map, key);
            }
            if ("multimap".equals(type)) {
                MultiMap multiMap = factory.getMultiMap(name);
                clientEndpoint.removeThisListener(multiMap, key);
            } else if ("topic".equals(type)) {
                ITopic topic = factory.getTopic(name);
                topic.removeMessageListener(clientEndpoint.messageListeners.remove(topic));
            } else if ("queue".equals(type)) {
                IQueue queue = factory.getQueue(name);
                queue.removeItemListener(clientEndpoint.queueItemListeners.remove(queue));
            }
            return protocol.success();
        }

        public void processCall(Node node, Packet packet) {
            ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap map = (IMap) factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.removeThisListener(map, packet.getKeyData());
            }
            if (getInstanceType(packet.name).equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.removeThisListener(multiMap, packet.getKeyData());
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic topic = (ITopic) factory.getOrCreateProxyByName(packet.name);
                topic.removeMessageListener(clientEndpoint.messageListeners.remove(topic));
            } else if (getInstanceType(packet.name).equals(InstanceType.QUEUE)) {
                IQueue queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
                queue.removeItemListener(clientEndpoint.queueItemListeners.remove(queue));
            }
        }
    }

    private class ListAddHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
            IList list = (IList) factory.getOrCreateProxyByName(packet.name);
            Boolean value = list.add(packet.getKeyData());
            packet.clearForResponse();
            packet.setValue(toData(value));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            boolean added = node.factory.getList(protocol.getArgs()[0]).add(new Data(protocol.getBuffers()[0].array()));
            return protocol.success(String.valueOf(added));
        }
    }

    private class SetAddHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
            ISet list = (ISet) factory.getOrCreateProxyByName(packet.name);
            boolean value = list.add(packet.getKeyData());
            packet.clearForResponse();
            packet.setValue(toData(value));
        }

        public Protocol processCall(Node node, Protocol protocol) {
            boolean added = node.factory.getSet(protocol.getArgs()[0]).add(new Data(protocol.getBuffers()[0].array()));
            return protocol.success(String.valueOf(added));
        }
    }

    private class MapItemRemoveHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Collection collection = (Collection) factory.getOrCreateProxyByName(packet.name);
            Data value = toData(collection.remove(packet.getKeyData()));
            packet.clearForResponse();
            packet.setValue(value);
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String[] args = protocol.getArgs();
            if (args.length < 4) {
            }
            String type = args[0];
            String name = args[1];
            boolean result = false;
            if ("set".equals(type)) {
                ISet set = node.factory.getSet(name);
                result = set.remove(new Data(protocol.getBuffers()[0].array()));
            } else if ("list".equals(type)) {
                IList list = node.factory.getList(name);
                result = list.remove(new Data(protocol.getBuffers()[0].array()));
            }
            return protocol.success(String.valueOf(result));
        }
    }

    public abstract class ClientOperationHandler implements CommandHandler {
        public abstract void processCall(Node node, Packet packet);

        public Protocol processCall(Node node, Protocol protocol) {
            return protocol;
        }

        public void handle(Node node, Packet packet) {
            try {
                processCall(node, packet);
            } catch (RuntimeException e) {
                logger.log(Level.WARNING,
                        "exception during handling " + packet.operation + ": " + e.getMessage(), e);
                packet.clearForResponse();
                packet.setValue(toData(new ClientServiceException(e)));
            }
            packet.lockAddress = null;
            packet.responseType = RESPONSE_SUCCESS;
            packet.operation = RESPONSE;
            sendResponse(packet, packet.conn);
        }

        public void handle(Node node, Protocol protocol) {
            Protocol response;
            try {
                response = processCall(node, protocol);
            } catch (RuntimeException e) {
                logger.log(Level.WARNING,
                        "exception during handling " + protocol.getCommand() + ": " + e.getMessage(), e);
                response = new Protocol(protocol.getConnection(), "ERROR", new String[]{protocol.getFlag(), e.getMessage()});
            }
            sendResponse(response, protocol.getConnection());
        }

        protected void sendResponse(SocketWritable request, Connection conn) {
            if (conn != null && conn.live()) {
                conn.getWriteHandler().enqueueSocketWritable(request);
            } else {
                logger.log(Level.WARNING, "unable to send response " + request);
            }
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

        public Protocol processCall(Node node, Protocol protocol) {
            return protocol.error(null, "unknown_command", protocol.getCommand());
        }
    }

    private abstract class ClientMapOperationHandler extends ClientOperationHandler {
        public abstract Data processMapOp(IMap<Object, Object> map, Data key, Data value);

        @Override
        public Protocol processCall(Node node, Protocol protocol) {
            String name = protocol.getArgs()[0];
            Data key = null;
            Data value = null;
            if (protocol.getBuffers() != null && protocol.getBuffers().length > 0) {
                key = new Data(protocol.getBuffers()[0].array());
                if (protocol.getBuffers().length > 1) {
                    value = new Data(protocol.getBuffers()[1].array());
                }
            }
            IMap<Object, Object> map = (IMap) factory.getMap(name);
            return processMapOp(protocol, map, key, value);
        }

        public Protocol processMapOp(Protocol protocol, IMap<Object, Object> map, Data key, Data value) {
            Data result = processMapOp(map, key, value);
            return protocol.success((result == null) ? null : ByteBuffer.wrap(result.buffer));
        }

        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
            packet.clearForResponse();
            packet.setValue(value);
        }
    }

    private abstract class ClientMapOperationHandlerWithTTL extends ClientOperationHandler {
        public void processCall(Node node, final Packet packet) {
            final IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(packet.name);
            final long ttl = packet.timeout;
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData(), ttl);
            packet.clearForResponse();
            packet.setValue(value);
        }

        public Protocol processCall(Node node, Protocol protocol) {
            String[] args = protocol.getArgs();
            final IMap<Object, Object> map = (IMap) factory.getMap(args[0]);
            final long ttl = (args.length > 1) ? Long.valueOf(args[1]) : 0;
            Data value = processMapOp(map, new Data(protocol.getBuffers()[0].array()), protocol.getBuffers().length > 1 ? new Data(protocol.getBuffers()[1].array()) : null, ttl);
            return protocol.success((value == null) ? null : ByteBuffer.wrap(value.buffer));
        }

        protected abstract Data processMapOp(IMap<Object, Object> map, Data keyData, Data valueData, long ttl);
    }

    private abstract class ClientQueueOperationHandler extends ClientOperationHandler {
        public abstract Data processQueueOp(IQueue<Object> queue, Data key, Data value);

        public void processCall(Node node, Packet packet) {
            IQueue<Object> queue = (IQueue) factory.getOrCreateProxyByName(packet.name);
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
            Transaction transaction = factory.getTransaction();
            processTransactionOp(transaction);
        }

        public Protocol processCall(Node node, Protocol protocol) {
            Transaction transaction = factory.getTransaction();
            processTransactionOp(transaction);
            return protocol.success();
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

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

import com.hazelcast.core.*;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.BaseManager.EventTask;
import com.hazelcast.impl.ConcurrentMapManager.Entries;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl.CollectionProxyReal;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;

import java.util.*;
import java.util.concurrent.*;

import static com.hazelcast.impl.BaseManager.getInstanceType;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ClientService {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new HashMap<Connection, ClientEndpoint>();
    private final ClientOperationHandler[] clientOperationHandlers = new ClientOperationHandler[300];

    public ClientService(Node node) {
        this.node = node;
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_PUT.getValue()] = new MapPutHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_PUT_MULTI.getValue()] = new MapPutMultiHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT.getValue()] = new MapPutIfAbsentHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_GET.getValue()] = new MapGetHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE.getValue()] = new MapRemoveHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE_IF_SAME.getValue()] = new MapRemoveIfSameHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE_MULTI.getValue()] = new MapRemoveMultiHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_EVICT.getValue()] = new MapEvictHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REPLACE_IF_NOT_NULL.getValue()] = new MapReplaceIfNotNullHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REPLACE_IF_SAME.getValue()] = new MapReplaceIfSameHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_SIZE.getValue()] = new MapSizeHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_GET_MAP_ENTRY.getValue()] = new GetMapEntryHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_LOCK.getValue()] = new MapLockHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_UNLOCK.getValue()] = new MapUnlockHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_CONTAINS.getValue()] = new MapContainsHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE.getValue()] = new MapContainsValueHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ADD_TO_LIST.getValue()] = new ListAddHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ADD_TO_SET.getValue()] = new SetAddHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM.getValue()] = new MapItemRemoveHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS.getValue()] = new MapIterateKeysHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_VALUE_COUNT.getValue()] = new MapValueCountHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_OFFER.getValue()] = new QueueOfferHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_POLL.getValue()] = new QueuePollHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_REMOVE.getValue()] = new QueueRemoveHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_PEEK.getValue()] = new QueuePeekHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_SIZE.getValue()] = new QueueSizeHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_PUBLISH.getValue()] = new QueuePublishHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_BEGIN.getValue()] = new TransactionBeginHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_COMMIT.getValue()] = new TransactionCommitHandler();
        clientOperationHandlers[ClusterOperation.TRANSACTION_ROLLBACK.getValue()] = new TransactionRollbackHandler();
        clientOperationHandlers[ClusterOperation.ADD_LISTENER.getValue()] = new AddListenerHandler();
        clientOperationHandlers[ClusterOperation.REMOVE_LISTENER.getValue()] = new RemoveListenerHandler();
        clientOperationHandlers[ClusterOperation.REMOTELY_PROCESS.getValue()] = new RemotelyProcessHandler();
        clientOperationHandlers[ClusterOperation.DESTROY.getValue()] = new DestroyHandler();
        clientOperationHandlers[ClusterOperation.GET_ID.getValue()] = new GetIdHandler();
        clientOperationHandlers[ClusterOperation.ADD_INDEX.getValue()] = new AddIndexHandler();
        clientOperationHandlers[ClusterOperation.NEW_ID.getValue()] = new NewIdHandler();
        clientOperationHandlers[ClusterOperation.REMOTELY_EXECUTE.getValue()] = new ExecutorServiceHandler();
        clientOperationHandlers[ClusterOperation.GET_INSTANCES.getValue()] = new GetInstancesHandler();
        clientOperationHandlers[ClusterOperation.GET_MEMBERS.getValue()] = new GetMembersHandler();
        clientOperationHandlers[ClusterOperation.GET_CLUSTER_TIME.getValue()] = new GetClusterTimeHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_AUTHENTICATE.getValue()] = new ClientAuthenticateHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_ADD_INSTANCE_LISTENER.getValue()] = new ClientAddInstanceListenerHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_ADD_MEMBERSHIP_LISTENER.getValue()] = new ClientAddMembershipListenerHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_GET_PARTITIONS.getValue()] = new GetPartitionsHandler();
    }
    // always called by InThread

    public void handle(Packet packet) {
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, packet, callContext, clientOperationHandlers);
        if (!packet.operation.equals(ClusterOperation.CONCURRENT_MAP_UNLOCK)) {
            node.clusterManager.enqueueEvent(clientEndpoint.hashCode(), clientRequestHandler);
        } else {
            node.executorManager.executeMigrationTask(clientRequestHandler);
        }
    }

    public ClientEndpoint getClientEndpoint(Connection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(conn);
            mapClientEndpoints.put(conn, clientEndpoint);
        }
        return clientEndpoint;
    }

    class ClientEndpoint implements EntryListener, InstanceListener, MembershipListener {
        final Connection conn;
        final private Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>();
        final ConcurrentMap<String, ConcurrentMap<Object, EntryEvent>> listeneds = new ConcurrentHashMap<String, ConcurrentMap<Object, EntryEvent>>();
        final Map<String, MessageListener<Object>> messageListeners = new HashMap<String, MessageListener<Object>>();

        ClientEndpoint(Connection conn) {
            this.conn = conn;
        }

        public CallContext getCallContext(int threadId) {
            CallContext context = callContexts.get(threadId);
            if (context == null) {
                int locallyMappedThreadId = ThreadContext.get().createNewThreadId();
                context = new CallContext(locallyMappedThreadId, true);
                callContexts.put(threadId, context);
            }
            return context;
        }

        public void addThisAsListener(IMap map, Data key, boolean includeValue) {
            if (key == null) {
                map.addEntryListener(this, includeValue);
            } else {
                map.addEntryListener(this, key, includeValue);
            }
        }

        private ConcurrentMap<Object, EntryEvent> getEventProcessedLog(String name) {
            ConcurrentMap<Object, EntryEvent> eventProcessedLog = listeneds.get(name);
            if (eventProcessedLog == null) {
                eventProcessedLog = new ConcurrentHashMap<Object, EntryEvent>();
                listeneds.putIfAbsent(name, eventProcessedLog);
            }
            return eventProcessedLog;
        }

        @Override
        public int hashCode() {
            return this.conn.hashCode();
        }

        public void entryAdded(EntryEvent event) {
            processEvent(event);
        }

        public void entryEvicted(EntryEvent event) {
            processEvent(event);
        }

        public void entryRemoved(EntryEvent event) {
            processEvent(event);
        }

        public void entryUpdated(EntryEvent event) {
            processEvent(event);
        }

        public void instanceCreated(InstanceEvent event) {
            processEvent(event);
        }

        public void instanceDestroyed(InstanceEvent event) {
            processEvent(event);
        }

        public void memberAdded(MembershipEvent membershipEvent) {
            processEvent(membershipEvent);
        }

        public void memberRemoved(MembershipEvent membershipEvent) {
            processEvent(membershipEvent);
        }

        private void processEvent(MembershipEvent membershipEvent) {
            Packet packet = createMembershipEventPacket(membershipEvent);
            sendPacket(packet);
        }

        private void processEvent(InstanceEvent event) {
            Packet packet = createInstanceEventPacket(event);
            sendPacket(packet);
        }

        /**
         * if a client is listening for both key and the entire
         * map, then we should make sure that we don't send
         * two separate events. One is enough. so check
         * if we already sent one.
         * <p/>
         * called by executor service threads
         *
         * @param event
         */
        private void processEvent(EntryEvent event) {
            final Object key = event.getKey();
            Map<Object, EntryEvent> eventProcessedLog = getEventProcessedLog(event.getName());
            if (eventProcessedLog.get(key) != null && eventProcessedLog.get(key) == event) {
                return;
            }
            eventProcessedLog.put(key, event);
            Packet packet = createEntryEventPacket(event);
            sendPacket(packet);
        }

        private void sendPacket(Packet packet) {
            if (conn != null && conn.live()) {
                conn.getWriteHandler().enqueuePacket(packet);
            }
        }

        private Packet createEntryEventPacket(EntryEvent event) {
            Packet packet = new Packet();
            EventTask eventTask = (EventTask) event;
            packet.set(event.getName(), ClusterOperation.EVENT, eventTask.getDataKey(), eventTask.getDataValue());
            packet.longValue = event.getEventType().getType();
            return packet;
        }

        private Packet createInstanceEventPacket(InstanceEvent event) {
            Packet packet = new Packet();
            packet.set(null, ClusterOperation.EVENT, toData(event.getInstance().getId()), toData(event.getEventType()));
            return packet;
        }

        private Packet createMembershipEventPacket(MembershipEvent membershipEvent) {
            Packet packet = new Packet();
            packet.set(null, ClusterOperation.EVENT, toData(membershipEvent.getMember()), toData(membershipEvent.getEventType()));
            return packet;
        }
    }

    public void reset() {
        mapClientEndpoints.clear();
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
            return (Data) queue.remove();
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

    private class QueuePublishHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ITopic<Object> topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
            topic.publish(packet.key);
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
            packet.value = toData(idGen.newId());
        }
    }

    private class MapPutMultiHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.value = toData(multiMap.put(packet.key, packet.value));
        }
    }

    private class MapValueCountHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.value = toData(multiMap.valueCount(packet.key));
        }
    }

    private class MapRemoveMultiHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            if (packet.value == null || packet.value.size == 0) {
                FactoryImpl.MultiMapProxy mmProxy = (FactoryImpl.MultiMapProxy) multiMap;
                FactoryImpl.MultiMapProxy.MultiMapBase base = mmProxy.getBase();
                MProxy mapProxy = base.mapProxy;
                packet.value = (Data) mapProxy.remove(packet.key);
            } else {
                packet.value = toData(multiMap.remove(packet.key, packet.value));
            }
        }
    }

    private class ExecutorServiceHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Future<?> future = null;
            ExecutorService executorService = node.factory.getExecutorService();
            Callable<Object> callable = (Callable<Object>) toObject(packet.key);
            Object result;
            try {
                if (callable instanceof ClientDistributedTask) {
                    DistributedTask task = null;
                    ClientDistributedTask cdt = (ClientDistributedTask) callable;
                    if (cdt.getKey() != null) {
                        task = new DistributedTask(cdt.getCallable(), cdt.getKey());
                    } else if (cdt.getMember() != null) {
                        task = new DistributedTask(cdt.getCallable(), cdt.getMember());
                    } else if (cdt.getMembers() != null) {
                        task = new MultiTask(cdt.getCallable(), cdt.getMembers());
                    }
                    executorService.submit(task);
                    result = task.get();
                } else {
                    future = executorService.submit(callable);
                    result = future.get();
                }
                packet.value = toData(result);
            } catch (InterruptedException e) {
                return;
            } catch (ExecutionException e) {
                e.printStackTrace();
                packet.value = toData(e);
            }
        }
    }

    private class GetInstancesHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Collection<Instance> instances = node.factory.getInstances();
            Object[] instanceIds = new Object[instances.size()];
            int counter = 0;
            for (Iterator<Instance> instanceIterator = instances.iterator(); instanceIterator.hasNext();) {
                Instance instance = instanceIterator.next();
                instanceIds[counter] = instance.getId();
                counter++;
            }
            packet.value = toData(instanceIds);
        }
    }

    private class GetMembersHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Cluster cluster = node.factory.getCluster();
            Set<Member> members = cluster.getMembers();
            Set<Data> setData = new LinkedHashSet<Data>();
            for (Iterator<Member> iterator = members.iterator(); iterator.hasNext();) {
                Member member = iterator.next();
                setData.add(toData(member));
            }
            Keys keys = new Keys(setData);
            packet.value = toData(keys);
        }
    }
    private class GetPartitionsHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            PartitionService partitionService = node.factory.getPartitionService();
            Set<Partition> partitions = partitionService.getPartitions();
            Set<Data> setData = new LinkedHashSet<Data>();
            for (Iterator<Partition> iterator = partitions.iterator(); iterator.hasNext();) {
                Partition partition = iterator.next();
                setData.add(toData(partition));
            }
            Keys keys = new Keys(setData);
            packet.value = toData(keys);
        }
    }

    private class GetClusterTimeHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Cluster cluster = node.factory.getCluster();
            long clusterTime = cluster.getClusterTime();
            packet.value = toData(clusterTime);
        }
    }

    private class ClientAuthenticateHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            String groupName = node.factory.getConfig().getGroupConfig().getName();
            String pass = node.factory.getConfig().getGroupConfig().getPassword();
            Boolean value = (groupName.equals(toObject(packet.key)) && pass.equals(toObject(packet.value)));
            packet.clearForResponse();
            packet.value = toData(value);
        }
    }

    private class ClientAddInstanceListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
//            System.out.println("Add listener");
            ClientEndpoint endPoint = getClientEndpoint(packet.conn);
            node.factory.addInstanceListener(endPoint);
        }
    }

    private class ClientAddMembershipListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ClientEndpoint endPoint = getClientEndpoint(packet.conn);
            node.factory.getCluster().addMembershipListener(endPoint);
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

    private class MapPutHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            MProxy mproxy = (MProxy) map;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                return (Data) map.put(key, toObject(value));
            } else {
                return (Data) map.put(key, value);
            }
        }
    }

    private class MapPutIfAbsentHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            MProxy mproxy = (MProxy) map;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                return (Data) map.putIfAbsent(key, toObject(value));
            } else {
                return (Data) map.putIfAbsent(key, value);
            }
        }
    }

    private class MapGetHandler extends ClientOperationHandler {

        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.value = (Data) map.get(packet.key);
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                FactoryImpl.MultiMapProxy multiMap = (FactoryImpl.MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
                FactoryImpl.MultiMapProxy.MultiMapBase base = multiMap.getBase();
                MProxy mapProxy = base.mapProxy;
                packet.value = (Data) mapProxy.get(packet.key);
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
                packet.value = toData(map.containsKey(packet.key));
            } else if (instanceType.equals(InstanceType.LIST) || instanceType.equals(InstanceType.SET)) {
                Collection<Object> collection = (Collection) node.factory.getOrCreateProxyByName(packet.name);
                packet.value = toData(collection.contains(packet.key));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.value = toData(multiMap.containsKey(packet.key));
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
                packet.value = toData(map.containsValue(packet.value));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
                if (packet.key != null && packet.key.size() > 0) {
                    packet.value = toData(multiMap.containsEntry(packet.key, packet.value));
                } else {
                    packet.value = toData(multiMap.containsValue(packet.value));
                }
            }
        }
    }

    private class MapSizeHandler extends ClientCollectionOperationHandler {
        @Override
        public void doListOp(Node node, Packet packet) {
            IList<Object> list = (IList) node.factory.getOrCreateProxyByName(packet.name);
            packet.value = toData(list.size());
        }

        @Override
        public void doMapOp(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.value = toData(map.size());
        }

        @Override
        public void doSetOp(Node node, Packet packet) {
            ISet<Object> set = (ISet) node.factory.getOrCreateProxyByName(packet.name);
            packet.value = toData(set.size());
        }

        @Override
        public void doMultiMapOp(Node node, Packet packet) {
            MultiMap multiMap = (MultiMap) node.factory.getOrCreateProxyByName(packet.name);
            packet.value = toData(multiMap.size());
        }

        @Override
        public void doQueueOp(Node node, Packet packet) {
            //ignore
        }
    }

    private class GetMapEntryHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            return toData(map.getMapEntry(key));
        }
    }

    private class MapLockHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            throw new RuntimeException("Shouldn't invoke this method");
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            long timeout = packet.timeout;
            Data value = null;
            if (timeout == -1) {
                map.lock(packet.key);
                value = null;
            } else if (timeout == 0) {
                value = toData(map.tryLock(packet.key));
            } else {
                value = toData(map.tryLock(packet.key, timeout, (TimeUnit) toObject(packet.value)));
            }
            packet.value = value;
        }
    }

    private class MapUnlockHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.unlock(key);
            return null;
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

    private class MapIterateKeysHandler extends ClientCollectionOperationHandler {
        public Data getMapKeys(IMap<Object, Object> map, Data key, Data value, Collection<Data> collection) {
            ConcurrentMapManager.Entries entries = null;
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
                keys.addKey(entry.getKeyData());
            }
            return toData(keys);
        }

        @Override
        public void doListOp(Node node, Packet packet) {
            CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            MProxy mapProxy = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
            packet.value = getMapKeys((IMap) mapProxy, packet.key, packet.value, new ArrayList<Data>());
        }

        @Override
        public void doMapOp(Node node, Packet packet) {
            packet.value = getMapKeys((IMap) node.factory.getOrCreateProxyByName(packet.name), packet.key, packet.value, new HashSet<Data>());
        }

        @Override
        public void doSetOp(Node node, Packet packet) {
            CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            MProxy mapProxy = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
            packet.value = getMapKeys((IMap) mapProxy, packet.key, packet.value, new HashSet<Data>());
        }

        public void doMultiMapOp(Node node, Packet packet) {
            FactoryImpl.MultiMapProxy multiMap = (FactoryImpl.MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
            FactoryImpl.MultiMapProxy.MultiMapBase base = multiMap.getBase();
            MProxy mapProxy = base.mapProxy;
            Data value = getMapKeys(mapProxy, packet.key, packet.value, new HashSet<Data>());
            packet.clearForResponse();
            packet.value = value;
        }

        public void doQueueOp(Node node, Packet packet) {
            IQueue queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
        }
    }

    private class AddListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, final Packet packet) {
            final ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            boolean includeValue = (int) packet.longValue == 1;
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.addThisAsListener(map, packet.key, includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.LIST) || getInstanceType(packet.name).equals(InstanceType.SET)) {
                CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
                IMap map = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
                clientEndpoint.addThisAsListener(map, null, includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.LIST)) {
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic<Object> topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
                final String packetName = packet.name;
                MessageListener<Object> messageListener = new MessageListener<Object>() {
                    public void onMessage(Object msg) {
                        Packet p = new Packet();
                        p.set(packetName, ClusterOperation.EVENT, msg, null);
                        clientEndpoint.sendPacket(p);
                    }
                };
                topic.addMessageListener(messageListener);
                clientEndpoint.messageListeners.put(packet.name, messageListener);
            }
            packet.clearForResponse();
        }
    }

    private class RemoveListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                if (packet.key == null) {
                    map.removeEntryListener(clientEndpoint);
                } else {
                    map.removeEntryListener(clientEndpoint, packet.key);
                }
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
                topic.removeMessageListener(clientEndpoint.messageListeners.remove(packet.name));
            }
        }
    }

    private class ListAddHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
            IList list = (IList) node.factory.getOrCreateProxyByName(packet.name);
            Boolean value = list.add(packet.key);
            packet.clearForResponse();
            packet.value = toData(value);
        }
    }

    private class SetAddHandler extends ClientOperationHandler {
        @Override
        public void processCall(Node node, Packet packet) {
            ISet list = (ISet) node.factory.getOrCreateProxyByName(packet.name);
            Boolean value = list.add(packet.key);
            packet.clearForResponse();
            packet.value = toData(value);
        }
    }

    private class MapItemRemoveHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            Collection collection = (Collection) node.factory.getOrCreateProxyByName(packet.name);
            Data value = toData(collection.remove(packet.key));
            packet.clearForResponse();
            packet.value = value;
        }
    }

    public abstract class ClientOperationHandler {
        public abstract void processCall(Node node, Packet packet);

        public void handle(Node node, Packet packet) {
            processCall(node, packet);
            sendResponse(packet);
        }

        protected void sendResponse(Packet request) {
            request.lockAddress = null;
            request.operation = ClusterOperation.RESPONSE;
            request.responseType = RESPONSE_SUCCESS;
            if (request.conn != null && request.conn.live()) {
                request.conn.getWriteHandler().enqueuePacket(request);
            }
        }
    }

    private abstract class ClientMapOperationHandler extends ClientOperationHandler {
        public abstract Data processMapOp(IMap<Object, Object> map, Data key, Data value);

        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            Data value = processMapOp(map, packet.key, packet.value);
            packet.clearForResponse();
            packet.value = value;
        }
    }

    private abstract class ClientQueueOperationHandler extends ClientOperationHandler {
        public abstract Data processQueueOp(IQueue<Object> queue, Data key, Data value);

        public void processCall(Node node, Packet packet) {
            IQueue<Object> queue = (IQueue) node.factory.getOrCreateProxyByName(packet.name);
            Data value = processQueueOp(queue, packet.key, packet.value);
            packet.clearForResponse();
            packet.value = value;
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
}

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
import com.hazelcast.impl.ConcurrentMapManager.Entries;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl;
import com.hazelcast.impl.FactoryImpl.CollectionProxyImpl.CollectionProxyReal;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.impl.BaseManager.getInstanceType;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ClientService implements ConnectionListener {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new ConcurrentHashMap<Connection, ClientEndpoint>();
    private final ClientOperationHandler[] clientOperationHandlers = new ClientOperationHandler[300];
    private final ILogger logger;

    public ClientService(Node node) {
        this.node = node;
        this.logger = node.getLogger(this.getClass().getName());
        node.getClusterImpl().addMembershipListener(new ClientServiceMembershipListener());
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
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_LOCK_MAP.getValue()] = new MapLockMapHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_UNLOCK_MAP.getValue()] = new MapUnlockMapHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_CONTAINS.getValue()] = new MapContainsHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE.getValue()] = new MapContainsValueHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ADD_TO_LIST.getValue()] = new ListAddHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ADD_TO_SET.getValue()] = new SetAddHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM.getValue()] = new MapItemRemoveHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS.getValue()] = new MapIterateKeysHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES.getValue()] = new MapIterateEntriesHandler();
        clientOperationHandlers[ClusterOperation.CONCURRENT_MAP_VALUE_COUNT.getValue()] = new MapValueCountHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_OFFER.getValue()] = new QueueOfferHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_POLL.getValue()] = new QueuePollHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_REMOVE.getValue()] = new QueueRemoveHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_PEEK.getValue()] = new QueuePeekHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_SIZE.getValue()] = new QueueSizeHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_PUBLISH.getValue()] = new QueuePublishHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_RAMAINING_CAPACITY.getValue()] = new QueueRemainingCapacityHandler();
        clientOperationHandlers[ClusterOperation.BLOCKING_QUEUE_ENTRIES.getValue()] = new QueueEntriesHandler();
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
        clientOperationHandlers[ClusterOperation.EXECUTE.getValue()] = new ExecutorServiceHandler();
        clientOperationHandlers[ClusterOperation.GET_INSTANCES.getValue()] = new GetInstancesHandler();
        clientOperationHandlers[ClusterOperation.GET_MEMBERS.getValue()] = new GetMembersHandler();
        clientOperationHandlers[ClusterOperation.GET_CLUSTER_TIME.getValue()] = new GetClusterTimeHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_AUTHENTICATE.getValue()] = new ClientAuthenticateHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_ADD_INSTANCE_LISTENER.getValue()] = new ClientAddInstanceListenerHandler();
        clientOperationHandlers[ClusterOperation.CLIENT_GET_PARTITIONS.getValue()] = new GetPartitionsHandler();
        node.connectionManager.addConnectionListener(this);
    }
    // always called by InThread

    public void handle(Packet packet) {
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, packet, callContext, clientOperationHandlers);
        if (packet.operation.equals(ClusterOperation.CONCURRENT_MAP_UNLOCK)) {
            node.executorManager.executeNow(clientRequestHandler);
        } else {
            node.executorManager.getClientExecutorService().executeOrderedRunnable(callContext.getThreadId(), clientRequestHandler);
        }
    }

    public int numberOfConnectedClients() {
        return mapClientEndpoints.size();
    }

    public ClientEndpoint getClientEndpoint(Connection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(conn);
            mapClientEndpoints.put(conn, clientEndpoint);
        }
        return clientEndpoint;
    }

    public void reset() {
        mapClientEndpoints.clear();
    }

    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(Connection connection) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.remove(connection);
        if (clientEndpoint != null) {
            clientEndpoint.connectionRemoved(connection);
        }
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
                return toData(queue.remove(toObject(value)));
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

    private class QueuePublishHandler extends ClientOperationHandler {
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
                FactoryImpl.MultiMapProxy mmProxy = (FactoryImpl.MultiMapProxy) multiMap;
                FactoryImpl.MultiMapProxy.MultiMapBase base = mmProxy.getBase();
                MProxy mapProxy = base.mapProxy;
                packet.setValue((Data) mapProxy.remove(packet.getKeyData()));
            } else {
                packet.setValue(toData(multiMap.remove(packet.getKeyData(), packet.getValueData())));
            }
        }
    }

    private class ExecutorServiceHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ExecutorService executorService = node.factory.getExecutorService();
            try {
                ClientDistributedTask cdt = (ClientDistributedTask) toObject(packet.getKeyData());
                Object result;
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
                executorService.execute(task);
                result = task.get();
                packet.setValue(toData(result));
            } catch (InterruptedException e) {
                return;
            } catch (ExecutionException e) {
                packet.setValue(toData(e));
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
                Object id = instance.getId();
                if (id instanceof FactoryImpl.ProxyKey) {
                    Object key = ((FactoryImpl.ProxyKey) id).getKey();
                    if (key instanceof Instance)
                        id = key.toString();
                }
                instanceIds[counter] = id;
                counter++;
            }
            packet.setValue(toData(instanceIds));
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

    private class ClientAuthenticateHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            String nodeGroupName = node.factory.getConfig().getGroupConfig().getName();
            String nodeGroupPassword = node.factory.getConfig().getGroupConfig().getPassword();
            Object groupName = toObject(packet.getKeyData());
            Object groupPassword = toObject(packet.getValueData());
            boolean value = (nodeGroupName.equals(groupName) && nodeGroupPassword.equals(groupPassword));
            logger.log(Level.INFO, "received auth from " + packet.conn
                    + ", this group name:" + nodeGroupName + ", auth group name:" + groupName
                    + ", " + (value ?
                    "successfully authenticated" : "authentication failed"));
            packet.clearForResponse();
            packet.setValue(toData(value));
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

    private class MapPutIfAbsentHandler extends ClientOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value, long ttl) {
            MProxy mproxy = (MProxy) map;
            Object v = value;
            if (node.concurrentMapManager.isMapIndexed(mproxy.getLongName())) {
                v = toObject(value);
            }
            if (ttl < 0) {
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

    private class MapGetHandler extends ClientOperationHandler {

        public void processCall(Node node, Packet packet) {
            InstanceType instanceType = getInstanceType(packet.name);
            if (instanceType.equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                packet.setValue((Data) map.get(packet.getKeyData()));
            } else if (instanceType.equals(InstanceType.MULTIMAP)) {
                FactoryImpl.MultiMapProxy multiMap = (FactoryImpl.MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
                FactoryImpl.MultiMapProxy.MultiMapBase base = multiMap.getBase();
                MProxy mapProxy = base.mapProxy;
                packet.setValue((Data) mapProxy.get(packet.getKeyData()));
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
                map.lock(packet.getKeyData());
                value = null;
            } else if (timeout == 0) {
                value = toData(map.tryLock(packet.getKeyData()));
            } else {
                value = toData(map.tryLock(packet.getKeyData(), timeout, (TimeUnit) toObject(packet.getValueData())));
            }
            packet.setValue(value);
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            clientEndpoint.locked(map, packet.getKeyData(), packet.threadId);
        }
    }

    private class MapUnlockHandler extends ClientMapOperationHandler {
        public Data processMapOp(IMap<Object, Object> map, Data key, Data value) {
            map.unlock(key);
            return null;
        }

        @Override
        public void processCall(Node node, Packet packet) {
            IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
            Data value = processMapOp(map, packet.getKeyData(), packet.getValueData());
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            clientEndpoint.unlocked(map, packet.getKeyData(), packet.threadId);
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
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            clientEndpoint.locked(map, packet.getKeyData(), packet.threadId);
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
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            clientEndpoint.unlocked(map, packet.getKeyData(), packet.threadId);
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
            ConcurrentMapManager.Entries entries = null;
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
                keys.addKey(toData(entry));
            }
            return toData(keys);
        }

        @Override
        public void doListOp(final Node node, final Packet packet) {
            final CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
            final MProxy mapProxy = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
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
            final FactoryImpl.MultiMapProxy multiMap = (FactoryImpl.MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
            final FactoryImpl.MultiMapProxy.MultiMapBase base = multiMap.getBase();
            final MProxy mapProxy = base.mapProxy;
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
            FactoryImpl.MultiMapProxy multiMap = (FactoryImpl.MultiMapProxy) node.factory.getOrCreateProxyByName(packet.name);
            FactoryImpl.MultiMapProxy.MultiMapBase base = multiMap.getBase();
            MProxy mapProxy = base.mapProxy;
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
            final ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            boolean includeValue = (int) packet.longValue == 1;
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap<Object, Object> map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.addThisAsListener(map, packet.getKeyData(), includeValue);
            } else if (getInstanceType(packet.name).equals(InstanceType.LIST) || getInstanceType(packet.name).equals(InstanceType.SET)) {
                CollectionProxyImpl collectionProxy = (CollectionProxyImpl) node.factory.getOrCreateProxyByName(packet.name);
                IMap map = ((CollectionProxyReal) collectionProxy.getBase()).mapProxy;
                clientEndpoint.addThisAsListener(map, null, includeValue);
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
                clientEndpoint.messageListeners.put(topic, messageListener);
            }
            packet.clearForResponse();
        }
    }

    private class RemoveListenerHandler extends ClientOperationHandler {
        public void processCall(Node node, Packet packet) {
            ClientEndpoint clientEndpoint = node.clientService.getClientEndpoint(packet.conn);
            if (getInstanceType(packet.name).equals(InstanceType.MAP)) {
                IMap map = (IMap) node.factory.getOrCreateProxyByName(packet.name);
                clientEndpoint.removeThisListener(map, packet.getKeyData());
            } else if (getInstanceType(packet.name).equals(InstanceType.TOPIC)) {
                ITopic topic = (ITopic) node.factory.getOrCreateProxyByName(packet.name);
                topic.removeMessageListener(clientEndpoint.messageListeners.remove(topic));
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

        public final void handle(Node node, Packet packet) {
            processCall(node, packet);
            sendResponse(packet);
        }

        protected void sendResponse(Packet request) {
            request.lockAddress = null;
            request.operation = ClusterOperation.RESPONSE;
            request.responseType = RESPONSE_SUCCESS;
            if (request.conn != null && request.conn.live()) {
                request.conn.getWriteHandler().enqueueSocketWritable(request);
            } else {
                logger.log(Level.WARNING, "unable to send response " + request);
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

/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.collection.client.CollectionItemListenHandler;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.list.client.*;
import com.hazelcast.collection.multimap.ObjectMultiMapProxy;
import com.hazelcast.collection.multimap.client.*;
import com.hazelcast.collection.multimap.tx.TransactionalMultiMapProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.collection.set.client.*;
import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/1/13
 */
public class CollectionService implements ManagedService, RemoteService, MembershipAwareService,
        MigrationAwareService, EventPublishingService<CollectionEvent, EventListener>, ClientProtocolService, TransactionalService {

    public static final String SERVICE_NAME = "hz:impl:collectionService";
    private final NodeEngine nodeEngine;
    private final CollectionPartitionContainer[] partitionContainers;
    private final ConcurrentMap<ListenerKey, String> eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();
    private final ConcurrentMap<CollectionProxyId, LocalMultiMapStatsImpl> statsMap = new ConcurrentHashMap<CollectionProxyId, LocalMultiMapStatsImpl>(1000);
    private final ConcurrencyUtil.ConstructorFunction<CollectionProxyId, LocalMultiMapStatsImpl> localMultiMapStatsConstructorFunction = new ConcurrencyUtil.ConstructorFunction<CollectionProxyId, LocalMultiMapStatsImpl>() {
        public LocalMultiMapStatsImpl createNew(CollectionProxyId key) {
            return new LocalMultiMapStatsImpl();
        }
    };

    public CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        partitionContainers = new CollectionPartitionContainer[partitionCount];
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new CollectionPartitionContainer(this, i);
        }
    }

    public void reset() {
        for (CollectionPartitionContainer container : partitionContainers) {
            if (container != null) {
                container.destroy();
            }
        }
    }

    public void shutdown() {
        reset();
        for (int i = 0; i < partitionContainers.length; i++) {
            partitionContainers[i] = null;
        }
        eventRegistrations.clear();
    }

    public CollectionContainer getOrCreateCollectionContainer(int partitionId, CollectionProxyId proxyId) {
        return partitionContainers[partitionId].getOrCreateCollectionContainer(proxyId);
    }

    public CollectionPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    <V> Collection<V> createNew(CollectionProxyId proxyId) {
        CollectionProxy proxy = (CollectionProxy) nodeEngine.getProxyService().getDistributedObject(SERVICE_NAME, proxyId);
        return proxy.createNew();
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        CollectionProxyId collectionProxyId = (CollectionProxyId) objectId;
        final CollectionProxyType type = collectionProxyId.type;
        switch (type) {
            case MULTI_MAP:
                return new ObjectMultiMapProxy(this, nodeEngine, collectionProxyId);
            case LIST:
                return new ObjectListProxy(this, nodeEngine, collectionProxyId);
            case SET:
                return new ObjectSetProxy(this, nodeEngine, collectionProxyId);
            case QUEUE:
                return null;
        }
        throw new IllegalArgumentException();
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return createDistributedObject(objectId);
    }

    public void destroyDistributedObject(Object objectId) {
        CollectionProxyId collectionProxyId = (CollectionProxyId) objectId;
        for (CollectionPartitionContainer container : partitionContainers) {
            if (container != null) {
                container.destroyCollection(collectionProxyId);
            }
        }
    }

    public Set<Data> localKeySet(CollectionProxyId proxyId) {
        Set<Data> keySet = new HashSet<Data>();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        Address thisAddress = clusterService.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(i);
            CollectionPartitionContainer partitionContainer = getPartitionContainer(i);
            CollectionContainer collectionContainer = partitionContainer.getCollectionContainer(proxyId);
            if (collectionContainer == null) {
                continue;
            }
            if (partitionInfo.getOwner().equals(thisAddress)) {
                keySet.addAll(collectionContainer.keySet());
            }
        }
        getLocalMultiMapStatsImpl(proxyId).incrementOtherOperations();
        return keySet;
    }

    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public void addListener(String name, EventListener listener, Data key, boolean includeValue, boolean local) {
        ListenerKey listenerKey = new ListenerKey(name, key, listener);
        String id = eventRegistrations.putIfAbsent(listenerKey, "tempId");
        if (id != null) {
            return;
        }
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = null;
        if (local) {
            registration = eventService.registerLocalListener(SERVICE_NAME, name, new CollectionEventFilter(includeValue, key), listener);
        } else {
            registration = eventService.registerListener(SERVICE_NAME, name, new CollectionEventFilter(includeValue, key), listener);
        }
        eventRegistrations.put(listenerKey, registration.getId());
    }

    public void removeListener(String name, EventListener listener, Data key) {
        ListenerKey listenerKey = new ListenerKey(name, key, listener);
        String id = eventRegistrations.remove(listenerKey);
        if (id != null) {
            EventService eventService = nodeEngine.getEventService();
            eventService.deregisterListener(SERVICE_NAME, name, id);
        }
    }

    public void dispatchEvent(CollectionEvent event, EventListener listener) {
        if (listener instanceof EntryListener) {
            EntryListener entryListener = (EntryListener) listener;
            EntryEvent entryEvent = new EntryEvent(event.getProxyId().getName(), nodeEngine.getClusterService().getMember(event.getCaller()),
                    event.getEventType().getType(), nodeEngine.toObject(event.getKey()), nodeEngine.toObject(event.getValue()));
            if (event.eventType.equals(EntryEventType.ADDED)) {
                entryListener.entryAdded(entryEvent);
            } else if (event.eventType.equals(EntryEventType.REMOVED)) {
                entryListener.entryRemoved(entryEvent);
            }
            getLocalMultiMapStatsImpl(event.getProxyId()).incrementReceivedEvents();
        } else if (listener instanceof ItemListener) {
            ItemListener itemListener = (ItemListener) listener;
            ItemEvent itemEvent = new ItemEvent(event.getProxyId().getName(), event.eventType.getType(), nodeEngine.toObject(event.getValue()),
                    nodeEngine.getClusterService().getMember(event.getCaller()));
            if (event.eventType.getType() == ItemEventType.ADDED.getType()) {
                itemListener.itemAdded(itemEvent);
            } else {
                itemListener.itemRemoved(itemEvent);
            }
        }
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        int replicaIndex = event.getReplicaIndex();
        CollectionPartitionContainer partitionContainer = partitionContainers[event.getPartitionId()];
        Map<CollectionProxyId, Map> map = new HashMap<CollectionProxyId, Map>(partitionContainer.containerMap.size());
        for (Map.Entry<CollectionProxyId, CollectionContainer> entry : partitionContainer.containerMap.entrySet()) {
            CollectionProxyId proxyId = entry.getKey();
            CollectionContainer container = entry.getValue();
            if (container.config.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            map.put(proxyId, container.collections);
        }
        if (map.isEmpty()) {
            return null;
        }
        return new CollectionMigrationOperation(map);
    }

    public void insertMigratedData(int partitionId, Map<CollectionProxyId, Map> map) {
        for (Map.Entry<CollectionProxyId, Map> entry : map.entrySet()) {
            CollectionProxyId proxyId = entry.getKey();
            CollectionContainer container = getOrCreateCollectionContainer(partitionId, proxyId);
            Map<Data, CollectionWrapper> collections = entry.getValue();
            container.collections.putAll(collections);
        }
    }

    private void clearMigrationData(int partitionId, int copyBackReplicaIndex) {
        final CollectionPartitionContainer partitionContainer = partitionContainers[partitionId];
        if (copyBackReplicaIndex == -1) {
            partitionContainer.containerMap.clear();
            return;
        }
        for (CollectionContainer container : partitionContainer.containerMap.values()) {
            int totalBackupCount = container.config.getTotalBackupCount();
            if (totalBackupCount < copyBackReplicaIndex) {
                container.destroy();
            }
        }
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            if (event.getMigrationType() == MigrationType.MOVE) {
                clearMigrationData(event.getPartitionId(), -1);
            } else if (event.getMigrationType() == MigrationType.MOVE_COPY_BACK) {
                clearMigrationData(event.getPartitionId(), event.getCopyBackReplicaIndex());
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        clearMigrationData(event.getPartitionId(), -1);
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void memberRemoved(MembershipServiceEvent event) {
        // TODO: when a member dies;
        // * rollback transaction
        // * do not know ?
    }

    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        Map<Command, ClientCommandHandler> map = new HashMap<Command, ClientCommandHandler>();
        //Set commands
        map.put(Command.SADD, new SetAddHandler(this));
        map.put(Command.SSIZE, new SetSizeHandler(this));
        map.put(Command.SREMOVE, new SetRemoveHandler(this));
        map.put(Command.SCONTAINS, new SetContainsHandler(this));
        map.put(Command.SGETALL, new SetGetAllHandler(this));
        map.put(Command.SLISTEN, new CollectionItemListenHandler(this) {
            @Override
            protected CollectionProxyId getCollectionProxyId(Protocol protocol) {
                return new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, protocol.args[0], CollectionProxyType.SET);
            }
        });
        //List commands
        map.put(Command.LADD, new AddHandler(this));
        map.put(Command.LSIZE, new SizeHandler(this));
        map.put(Command.LREMOVE, new RemoveHandler(this));
        map.put(Command.LCONTAINS, new ContainsHandler(this));
        map.put(Command.LGETALL, new GetAllHandler(this));
        map.put(Command.LGET, new GetHandler(this));
        map.put(Command.LINDEXOF, new IndexOfHandler(this));
        map.put(Command.LLASTINDEXOF, new LastIndexOfHandler(this));
        map.put(Command.LSET, new ListSetHandler(this));
        map.put(Command.LLISTEN, new CollectionItemListenHandler(this) {
            @Override
            protected CollectionProxyId getCollectionProxyId(Protocol protocol) {
                return new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, protocol.args[0], CollectionProxyType.LIST);
            }
        });
        //MultiMap commands
        map.put(Command.MMPUT, new PutHandler(this));
        map.put(Command.MMGET, new MMGetHandler(this));
        map.put(Command.MMSIZE, new MMSizeHandler(this));
        map.put(Command.MMREMOVE, new MMRemoveHandler(this));
        map.put(Command.MMVALUECOUNT, new ValueCountHandler(this));
        map.put(Command.MMCONTAINSKEY, new ContainsKeyHandler(this));
        map.put(Command.MMCONTAINSVALUE, new ContainsValueHandler(this));
        map.put(Command.MMCONTAINSENTRY, new ContainsEntryHandler(this));
        map.put(Command.MMKEYS, new MMKeysHandler(this));
        map.put(Command.MMLOCK, new LockHandler(this));
        map.put(Command.MMUNLOCK, new UnlockHandler(this));
        map.put(Command.MMTRYLOCK, new TryLockHandler(this));
        map.put(Command.MMLISTEN, new ListenHandler(this));
        return map;
    }

    public void clientDisconnected(String clientUuid) {
    }

    public LocalMapStats createStats(CollectionProxyId proxyId) {
        LocalMultiMapStatsImpl stats = getLocalMultiMapStatsImpl(proxyId);
        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long dirtyCount = 0;
        long ownedEntryMemoryCost = 0;
        long backupEntryMemoryCost = 0;
        long hits = 0;
        long lockedEntryCount = 0;
        //TODO @msk memory costs????
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();

        Address thisAddress = clusterService.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(i);
            CollectionPartitionContainer partitionContainer = getPartitionContainer(i);
            CollectionContainer collectionContainer = partitionContainer.getCollectionContainer(proxyId);
            if (collectionContainer == null) {
                continue;
            }
            if (partitionInfo.getOwner().equals(thisAddress)) {
                lockedEntryCount += collectionContainer.getLockedCount();
                for (CollectionWrapper wrapper : collectionContainer.collections.values()) {
                    hits += wrapper.getHits();
                    ownedEntryCount += wrapper.getCollection().size();
                }
            } else {
                int backupCount = collectionContainer.config.getTotalBackupCount();
                for (int j = 1; j <= backupCount; j++) {
                    Address replicaAddress = partitionInfo.getReplicaAddress(j);
                    int memberSize = nodeEngine.getClusterService().getMembers().size();

                    int tryCount = 3;
                    // wait if the partition table is not updated yet
                    while (memberSize > backupCount && replicaAddress == null && tryCount-- > 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            ExceptionUtil.rethrow(e);
                        }
                        replicaAddress = partitionInfo.getReplicaAddress(j);
                    }

                    if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                        for (CollectionWrapper wrapper : collectionContainer.collections.values()) {
                            backupEntryCount += wrapper.getCollection().size();
                        }
                    }
                }
            }
        }
        stats.setOwnedEntryCount(ownedEntryCount);
        stats.setBackupEntryCount(backupEntryCount);
        stats.setHits(hits);
        stats.setLockedEntryCount(lockedEntryCount);
        return stats;
    }


    public LocalMultiMapStatsImpl getLocalMultiMapStatsImpl(CollectionProxyId name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localMultiMapStatsConstructorFunction);
    }

    public <T extends TransactionalObject> T createTransactionalObject(Object id, Transaction transaction) {
        CollectionProxyId collectionProxyId = (CollectionProxyId) id;
        final CollectionProxyType type = collectionProxyId.type;
        switch (type) {
            case MULTI_MAP:
                return (T) new TransactionalMultiMapProxy(nodeEngine, this, collectionProxyId, transaction);
            case LIST:
                return null;
            case SET:
                return null;
            case QUEUE:
                return null;
        }
        throw new IllegalArgumentException();
    }
}

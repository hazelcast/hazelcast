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

import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.multimap.ObjectMultiMapProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.core.*;
import com.hazelcast.map.LockInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.spi.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/1/13
 */
public class CollectionService implements ManagedService, RemoteService, EventPublishingService<CollectionEvent, EventListener>, MigrationAwareService {

    public static final String COLLECTION_SERVICE_NAME = "hz:impl:collectionService";

    private final NodeEngine nodeEngine;

    private final ConcurrentMap<ListenerKey, String> eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();

    private final CollectionPartitionContainer[] partitionContainers;

    public CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        partitionContainers = new CollectionPartitionContainer[nodeEngine.getPartitionCount()];
    }

    public CollectionContainer getOrCreateCollectionContainer(int partitionId, CollectionProxyId proxyId) {
        return partitionContainers[partitionId].getOrCreateCollectionContainer(proxyId);
    }

    public CollectionPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new CollectionPartitionContainer(this);
        }
    }

    public void destroy() {
    }

    Object createNew(CollectionProxyId proxyId) {
        CollectionProxy proxy = (CollectionProxy) nodeEngine.getProxyService().getDistributedObject(COLLECTION_SERVICE_NAME, proxyId);
        return proxy.createNew();
    }

    public String getServiceName() {
        return COLLECTION_SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        CollectionProxyId collectionProxyId = (CollectionProxyId) objectId;
        final String name = collectionProxyId.name;
        final CollectionProxyType type = collectionProxyId.type;
        switch (type) {
            case MULTI_MAP:
                return new ObjectMultiMapProxy(name, this, nodeEngine, collectionProxyId.type);
            case LIST:
                return new ObjectListProxy(name, this, nodeEngine, collectionProxyId.type);
            case SET:
                return new ObjectSetProxy(name, this, nodeEngine, collectionProxyId.type);
            case QUEUE:
                return null;
        }
        throw new IllegalArgumentException();
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return createDistributedObject(objectId);
    }

    public void destroyDistributedObject(Object objectId) {

    }

    public Set<Data> localKeySet(CollectionProxyId proxyId) {
        Set<Data> keySet = new HashSet<Data>();
        for (CollectionPartitionContainer partitionContainer : partitionContainers) {
            CollectionContainer container = partitionContainer.getOrCreateCollectionContainer(proxyId);
            keySet.addAll(container.keySet());
        }
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
            registration = eventService.registerLocalListener(COLLECTION_SERVICE_NAME, name, new CollectionEventFilter(includeValue, key), listener);
        } else {
            registration = eventService.registerListener(COLLECTION_SERVICE_NAME, name, new CollectionEventFilter(includeValue, key), listener);
        }

        eventRegistrations.put(listenerKey, registration.getId());
    }

    public void removeListener(String name, EventListener listener, Data key) {
        ListenerKey listenerKey = new ListenerKey(name, key, listener);
        String id = eventRegistrations.remove(listenerKey);
        if (id != null) {
            EventService eventService = nodeEngine.getEventService();
            eventService.deregisterListener(COLLECTION_SERVICE_NAME, name, id);
        }
    }

    public void dispatchEvent(CollectionEvent event, EventListener listener) {
        if (listener instanceof EntryListener) {
            EntryListener entryListener = (EntryListener) listener;
            EntryEvent entryEvent = new EntryEvent(event.getName(), nodeEngine.getCluster().getMember(event.getCaller()),
                    event.getEventType().getType(), nodeEngine.toObject(event.getKey()), nodeEngine.toObject(event.getValue()));


            if (event.eventType.equals(EntryEventType.ADDED)) {
                entryListener.entryAdded(entryEvent);
            } else if (event.eventType.equals(EntryEventType.REMOVED)) {
                entryListener.entryRemoved(entryEvent);
            }
        } else if (listener instanceof ItemListener) {
            ItemListener itemListener = (ItemListener) listener;
            ItemEvent itemEvent = new ItemEvent(event.getName(), event.eventType.getType(), nodeEngine.toObject(event.getValue()),
                    nodeEngine.getCluster().getMember(event.getCaller()));
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
        if (event.getPartitionId() < 0 || event.getPartitionId() >= nodeEngine.getPartitionCount()) {
            return null; // is it possible
        }
        int replicaIndex = event.getReplicaIndex();
        CollectionPartitionContainer partitionContainer = partitionContainers[event.getPartitionId()];
        Map<CollectionProxyId, Map[]> map = new HashMap<CollectionProxyId, Map[]>(partitionContainer.containerMap.size());
        for (Map.Entry<CollectionProxyId, CollectionContainer> entry : partitionContainer.containerMap.entrySet()) {
            CollectionProxyId proxyId = entry.getKey();
            CollectionContainer container = entry.getValue();
            if (container.config.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            map.put(proxyId, new Map[]{container.objects, container.locks});
        }
        if (map.isEmpty()) {
            return null;
        }
        return new CollectionMigrationOperation(map);
    }

    public void insertMigratedData(int partitionId, Map<CollectionProxyId, Map[]> map) {
        for (Map.Entry<CollectionProxyId, Map[]> entry : map.entrySet()) {
            CollectionProxyId proxyId = entry.getKey();
            CollectionContainer container = getOrCreateCollectionContainer(partitionId, proxyId);
            Map<Data, Object> objects = entry.getValue()[0];
            for (Map.Entry<Data, Object> objectEntry : objects.entrySet()) {
                Data key = objectEntry.getKey();
                Object object = objectEntry.getValue();
                if (object instanceof Collection) {
                    Collection coll = (Collection) createNew(proxyId);
                    coll.addAll((Collection) object);
                    container.objects.put(key, coll);
                } else {
                    container.objects.put(key, object);
                }
            }
            Map<Data, LockInfo> locks = entry.getValue()[1];
            container.locks.putAll(locks);
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
                container.clear();
            }
        }
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getMigrationType() == MigrationType.MOVE) {
            System.err.println("move");
        }
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

    public int getMaxBackupCount() {
        int max = 0;
        for (CollectionPartitionContainer partitionContainer : partitionContainers) {
            int c = partitionContainer.getMaxBackupCount();
            max = Math.max(max, c);
        }
        return max;
    }
}

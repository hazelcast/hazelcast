/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.*;

import java.util.*;

/**
 * @ali 8/29/13
 */
public abstract class CollectionService implements ManagedService, RemoteService, EventPublishingService<CollectionEvent, ItemListener>, TransactionalService, MigrationAwareService {

    protected static final StringPartitioningStrategy PARTITIONING_STRATEGY = new StringPartitioningStrategy();

    protected NodeEngine nodeEngine;

    protected CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        getContainerMap().clear();
    }

    public void shutdown() {
        reset();
    }

    public void destroyDistributedObject(Object objectId) {
        final String name = String.valueOf(objectId);
        getContainerMap().remove(name);
        nodeEngine.getEventService().deregisterAllListeners(getServiceName(), name);
    }

    public abstract CollectionContainer getOrCreateContainer(String name, boolean backup);
    public abstract Map<String, ? extends CollectionContainer> getContainerMap();
    public abstract String getServiceName();

    public void dispatchEvent(CollectionEvent event, ItemListener listener) {
        ItemEvent itemEvent = new ItemEvent(event.name, event.eventType, nodeEngine.toObject(event.data),
                nodeEngine.getClusterService().getMember(event.caller));
        if (event.eventType.equals(ItemEventType.ADDED)) {
            listener.itemAdded(itemEvent);
        } else {
            listener.itemRemoved(itemEvent);
        }
    }

    public void rollbackTransaction(String transactionId) {
        final Set<String> collectionNames = getContainerMap().keySet();
        for (String name : collectionNames) {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new CollectionTransactionRollbackOperation(name, transactionId).setPartitionId(partitionId).setService(this).setNodeEngine(nodeEngine);
            nodeEngine.getOperationService().executeOperation(operation);
        }
    }

    public void beforeMigration(PartitionMigrationEvent event) {
    }

    public Map<String, CollectionContainer> getMigrationData(PartitionReplicationEvent event) {
        Map<String, CollectionContainer> migrationData = new HashMap<String, CollectionContainer>();
        for (Map.Entry<String, ? extends CollectionContainer> entry : getContainerMap().entrySet()) {
            String name = entry.getKey();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            CollectionContainer container = entry.getValue();
            if (partitionId == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }
        return migrationData;
    }

    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    private void clearMigrationData(int partitionId) {
        final Set<? extends Map.Entry<String, ? extends CollectionContainer>> entrySet = getContainerMap().entrySet();
        final Iterator<? extends Map.Entry<String, ? extends CollectionContainer>> iterator = entrySet.iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, ? extends CollectionContainer> entry = iterator.next();
            final String name = entry.getKey();
            final CollectionContainer container = entry.getValue();
            int containerPartitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            if (containerPartitionId == partitionId) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public void  addContainer(String name, CollectionContainer container){
        final Map map = getContainerMap();
        map.put(name, container);
    }

}

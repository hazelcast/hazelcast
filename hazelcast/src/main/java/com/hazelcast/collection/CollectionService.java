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

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.collection.common.DataAwareItemEvent;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.TransactionalService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;

public abstract class CollectionService implements ManagedService, RemoteService,
        EventPublishingService<CollectionEvent, ItemListener>, TransactionalService, MigrationAwareService {

    protected final NodeEngine nodeEngine;

    private final ILogger logger;

    protected CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        getContainerMap().clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public void destroyDistributedObject(String name) {
        getContainerMap().remove(name);
        nodeEngine.getEventService().deregisterAllListeners(getServiceName(), name);
    }

    public abstract CollectionContainer getOrCreateContainer(String name, boolean backup);

    public abstract Map<String, ? extends CollectionContainer> getContainerMap();

    public abstract String getServiceName();

    @Override
    public void dispatchEvent(CollectionEvent event, ItemListener listener) {
        final MemberImpl member = nodeEngine.getClusterService().getMember(event.caller);
        ItemEvent itemEvent = new DataAwareItemEvent(event.name, event.eventType, event.data,
                member, nodeEngine.getSerializationService());
        if (member == null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Dropping event " + itemEvent + " from unknown address:" + event.caller);
            }
            return;
        }
        if (event.eventType.equals(ItemEventType.ADDED)) {
            listener.itemAdded(itemEvent);
        } else {
            listener.itemRemoved(itemEvent);
        }
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        final Set<String> collectionNames = getContainerMap().keySet();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();
        for (String name : collectionNames) {
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new CollectionTransactionRollbackOperation(name, transactionId)
                    .setPartitionId(partitionId)
                    .setService(this)
                    .setNodeEngine(nodeEngine);
            operationService.executeOperation(operation);
        }
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    public Map<String, CollectionContainer> getMigrationData(PartitionReplicationEvent event) {
        Map<String, CollectionContainer> migrationData = new HashMap<String, CollectionContainer>();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (Map.Entry<String, ? extends CollectionContainer> entry : getContainerMap().entrySet()) {
            String name = entry.getKey();
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            CollectionContainer container = entry.getValue();
            if (partitionId == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }
        return migrationData;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    private void clearMigrationData(int partitionId) {
        final Set<? extends Map.Entry<String, ? extends CollectionContainer>> entrySet = getContainerMap().entrySet();
        final Iterator<? extends Map.Entry<String, ? extends CollectionContainer>> iterator = entrySet.iterator();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        while (iterator.hasNext()) {
            final Map.Entry<String, ? extends CollectionContainer> entry = iterator.next();
            final String name = entry.getKey();
            final CollectionContainer container = entry.getValue();
            int containerPartitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            if (containerPartitionId == partitionId) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public void addContainer(String name, CollectionContainer container) {
        final Map map = getContainerMap();
        map.put(name, container);
    }

}

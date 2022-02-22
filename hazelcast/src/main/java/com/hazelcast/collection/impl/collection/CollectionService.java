/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.collection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.collection.operations.CollectionMergeOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionOperation;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTransactionRollbackOperation;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.AbstractLocalCollectionStats;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CollectionMergeTypes;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;

public abstract class CollectionService implements ManagedService, RemoteService, EventPublishingService<CollectionEvent,
        ItemListener<Data>>, TransactionalService, MigrationAwareService, SplitBrainProtectionAwareService,
        SplitBrainHandlerService {

    protected final NodeEngine nodeEngine;
    protected final SerializationService serializationService;
    protected final IPartitionService partitionService;

    private final ILogger logger;

    protected CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
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
    public void destroyDistributedObject(String name, boolean local) {
        CollectionContainer container = getContainerMap().remove(name);
        if (container != null) {
            container.destroy();
        }
        nodeEngine.getEventService().deregisterAllListeners(getServiceName(), name);
    }

    public abstract CollectionContainer getOrCreateContainer(String name, boolean backup);

    public abstract ConcurrentMap<String, ? extends CollectionContainer> getContainerMap();

    public abstract String getServiceName();

    @Override
    public void dispatchEvent(CollectionEvent event, ItemListener<Data> listener) {
        final MemberImpl member = nodeEngine.getClusterService().getMember(event.getCaller());
        ItemEvent<Data> itemEvent = new DataAwareItemEvent<Data>(event.getName(), event.getEventType(), event.getData(),
                member, serializationService);
        if (member == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Dropping event " + itemEvent + " from unknown address:" + event.getCaller());
            }
            return;
        }
        if (event.getEventType().equals(ItemEventType.ADDED)) {
            listener.itemAdded(itemEvent);
        } else {
            listener.itemRemoved(itemEvent);
        }
    }

    @Override
    public void rollbackTransaction(UUID transactionId) {
        final Set<String> collectionNames = getContainerMap().keySet();
        OperationService operationService = nodeEngine.getOperationService();
        for (String name : collectionNames) {
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new CollectionTransactionRollbackOperation(name, transactionId)
                    .setPartitionId(partitionId)
                    .setService(this)
                    .setNodeEngine(nodeEngine);
            operationService.invokeOnPartition(operation);
        }
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    protected Map<String, CollectionContainer> getMigrationData(PartitionReplicationEvent event) {
        Map<String, CollectionContainer> migrationData = new HashMap<String, CollectionContainer>();
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
            clearCollectionsHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearCollectionsHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearCollectionsHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        Set<? extends Map.Entry<String, ? extends CollectionContainer>> entrySet = getContainerMap().entrySet();
        Iterator<? extends Map.Entry<String, ? extends CollectionContainer>> iterator = entrySet.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ? extends CollectionContainer> entry = iterator.next();
            String name = entry.getKey();
            CollectionContainer container = entry.getValue();
            int containerPartitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            if (containerPartitionId != partitionId) {
                continue;
            }

            if (thresholdReplicaIndex < 0 || thresholdReplicaIndex > container.getConfig().getTotalBackupCount()) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public void addContainer(String name, CollectionContainer container) {
        getRawContainerMap().put(name, container);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, CollectionContainer> getRawContainerMap() {
        return (ConcurrentMap<String, CollectionContainer>) getContainerMap();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        CollectionContainerCollector collector = new CollectionContainerCollector(nodeEngine, getRawContainerMap());
        collector.run();
        return new Merger(collector);
    }

    public abstract AbstractLocalCollectionStats getLocalCollectionStats(String name);

    private class Merger extends AbstractContainerMerger<CollectionContainer, Collection<Object>, CollectionMergeTypes<Object>> {

        Merger(CollectionContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "collection";
        }

        @Override
        public void runInternal() {
            for (Map.Entry<Integer, Collection<CollectionContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<CollectionContainer> containerList = entry.getValue();
                for (CollectionContainer container : containerList) {
                    // TODO: add batching (which is a bit complex, since collections don't have a multi-name operation yet
                    Collection<CollectionItem> items = container.getCollection();

                    String name = container.getName();
                    SplitBrainMergePolicy<Collection<Object>, CollectionMergeTypes<Object>, Collection<Object>> mergePolicy
                            = getMergePolicy(container.getConfig().getMergePolicyConfig());

                    CollectionMergeTypes<Object> mergingValue = createMergingValue(serializationService, items);
                    sendBatch(partitionId, name, mergePolicy, mergingValue);

                    items.clear();
                }
            }
        }

        private void sendBatch(int partitionId, String name,
                               SplitBrainMergePolicy<Collection<Object>,
                                       CollectionMergeTypes<Object>, Collection<Object>> mergePolicy,
                               CollectionMergeTypes<Object> mergingValue) {
            CollectionOperation operation = new CollectionMergeOperation(name, mergePolicy, mergingValue);
            invoke(getServiceName(), operation, partitionId);
        }
    }
}

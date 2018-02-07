/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.collection.operations.CollectionMergeOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionOperation;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTransactionRollbackOperation;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;

public abstract class CollectionService implements ManagedService, RemoteService, EventPublishingService<CollectionEvent,
        ItemListener<Data>>, TransactionalService, MigrationAwareService, QuorumAwareService, SplitBrainHandlerService {

    protected final NodeEngine nodeEngine;
    protected final SerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final SplitBrainMergePolicyProvider mergePolicyProvider;

    private final ILogger logger;

    protected CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
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
    public void rollbackTransaction(String transactionId) {
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
        final Map map = getContainerMap();
        map.put(name, container);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        Map<String, ? extends CollectionContainer> containers = getContainerMap();
        Map<Integer, Map<CollectionContainer, List<CollectionItem>>> itemMap
                = createHashMap(partitionService.getPartitionCount());

        for (CollectionContainer container : containers.values()) {
            String name = container.getName();
            Data partitionAwareName = serializationService.toData(name, StringPartitioningStrategy.INSTANCE);
            int partitionId = partitionService.getPartitionId(partitionAwareName);
            if (partitionService.isPartitionOwner(partitionId)) {
                // add your owned entries to the map so they will be merged
                if (!(getMergePolicy(container) instanceof DiscardMergePolicy)) {
                    Map<CollectionContainer, List<CollectionItem>> containerMap = itemMap.get(partitionId);
                    if (containerMap == null) {
                        containerMap = new HashMap<CollectionContainer, List<CollectionItem>>();
                        itemMap.put(partitionId, containerMap);
                    }
                    containerMap.put(container, new ArrayList<CollectionItem>(container.getCollection()));
                }
            }
            // clear all items either owned or backup
            container.clear();
        }
        containers.clear();

        return new Merger(itemMap);
    }

    private SplitBrainMergePolicy getMergePolicy(CollectionContainer container) {
        String mergePolicyName = container.getConfig().getMergePolicyConfig().getPolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    private class Merger implements Runnable {

        private static final long TIMEOUT_FACTOR = 500;

        private final Map<Integer, Map<CollectionContainer, List<CollectionItem>>> itemMap;

        Merger(Map<Integer, Map<CollectionContainer, List<CollectionItem>>> itemMap) {
            this.itemMap = itemMap;
        }

        @Override
        public void run() {
            final ILogger logger = nodeEngine.getLogger(CollectionService.class);
            final Semaphore semaphore = new Semaphore(0);

            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            // we cannot merge into a 3.9 cluster, since not all members may understand the CollectionMergeOperation
            // RU_COMPAT_3_9
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                logger.info("Cluster needs to run version " + Versions.V3_10 + " to merge collection instances");
                return;
            }

            int itemCount = 0;
            int operationCount = 0;
            List<SplitBrainMergeEntryView<Long, Data>> mergeEntries;
            for (Map.Entry<Integer, Map<CollectionContainer, List<CollectionItem>>> partitionEntry : itemMap.entrySet()) {
                int partitionId = partitionEntry.getKey();
                Map<CollectionContainer, List<CollectionItem>> containerMap = partitionEntry.getValue();
                for (Map.Entry<CollectionContainer, List<CollectionItem>> containerEntry : containerMap.entrySet()) {
                    CollectionContainer container = containerEntry.getKey();
                    Collection<CollectionItem> itemList = containerEntry.getValue();

                    String name = container.getName();
                    int batchSize = container.getConfig().getMergePolicyConfig().getBatchSize();
                    SplitBrainMergePolicy mergePolicy = getMergePolicy(container);

                    mergeEntries = new ArrayList<SplitBrainMergeEntryView<Long, Data>>();
                    for (CollectionItem item : itemList) {
                        SplitBrainMergeEntryView<Long, Data> entryView = createSplitBrainMergeEntryView(item);
                        mergeEntries.add(entryView);
                        itemCount++;

                        if (mergeEntries.size() == batchSize) {
                            sendBatch(partitionId, name, mergePolicy, mergeEntries, mergeCallback);
                            mergeEntries = new ArrayList<SplitBrainMergeEntryView<Long, Data>>(batchSize);
                            operationCount++;
                        }
                    }
                    itemList.clear();
                    if (mergeEntries.size() > 0) {
                        sendBatch(partitionId, name, mergePolicy, mergeEntries, mergeCallback);
                        operationCount++;
                    }
                }
            }
            itemMap.clear();

            try {
                semaphore.tryAcquire(operationCount, itemCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for merge operation...");
            }
        }

        private void sendBatch(int partitionId, String name, SplitBrainMergePolicy mergePolicy,
                               List<SplitBrainMergeEntryView<Long, Data>> mergeEntries, ExecutionCallback<Object> mergeCallback) {
            CollectionOperation operation = new CollectionMergeOperation(name, mergePolicy, mergeEntries);
            try {
                nodeEngine.getOperationService()
                        .invokeOnPartition(getServiceName(), operation, partitionId)
                        .andThen(mergeCallback);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }
}

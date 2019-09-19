/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.queue.operations.QueueMergeOperation;
import com.hazelcast.collection.impl.queue.operations.QueueReplicationOperation;
import com.hazelcast.collection.impl.txnqueue.TransactionalQueueProxy;
import com.hazelcast.collection.impl.txnqueue.operations.QueueTransactionRollbackOperation;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.QueueMergeTypes;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationAwareService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.scheduler.EntryTaskScheduler;
import com.hazelcast.internal.util.scheduler.EntryTaskSchedulerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.ConfigValidator.checkQueueConfig;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.scheduler.ScheduleType.POSTPONE;

/**
 * Provides important services via methods for the the Queue
 * such as {@link com.hazelcast.collection.impl.queue.QueueEvictionProcessor }
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public class QueueService implements ManagedService, MigrationAwareService, TransactionalService, RemoteService,
        EventPublishingService<QueueEvent, ItemListener>, StatisticsAwareService<LocalQueueStats>,
        SplitBrainProtectionAwareService, SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:queueService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, QueueContainer> containerMap
            = new ConcurrentHashMap<String, QueueContainer>();
    private final ConcurrentMap<String, LocalQueueStatsImpl> statsMap
            = new ConcurrentHashMap<String, LocalQueueStatsImpl>(1000);
    private final ConstructorFunction<String, LocalQueueStatsImpl> localQueueStatsConstructorFunction
            = new ConstructorFunction<String, LocalQueueStatsImpl>() {
        @Override
        public LocalQueueStatsImpl createNew(String key) {
            return new LocalQueueStatsImpl();
        }
    };

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            QueueConfig queueConfig = nodeEngine.getConfig().findQueueConfig(name);
            String splitBrainProtectionName = queueConfig.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    private final NodeEngine nodeEngine;
    private final SerializationService serializationService;
    private final IPartitionService partitionService;
    private final ILogger logger;
    private final EntryTaskScheduler<String, Void> queueEvictionScheduler;

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(QueueService.class);
        TaskScheduler globalScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        QueueEvictionProcessor entryProcessor = new QueueEvictionProcessor(nodeEngine);
        this.queueEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(globalScheduler, entryProcessor, POSTPONE);
    }

    public void scheduleEviction(String name, long delay) {
        queueEvictionScheduler.schedule(delay, name, null);
    }

    public void cancelEviction(String name) {
        queueEvictionScheduler.cancel(name);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        containerMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    public QueueContainer getOrCreateContainer(final String name, boolean fromBackup) {
        QueueContainer container = containerMap.get(name);
        if (container != null) {
            return container;
        }

        container = new QueueContainer(name, nodeEngine.getConfig().findQueueConfig(name), nodeEngine, this);
        QueueContainer existing = containerMap.putIfAbsent(name, container);
        if (existing != null) {
            container = existing;
        } else {
            container.init(fromBackup);
            container.getStore().instrument(nodeEngine);
        }
        return container;
    }

    public void addContainer(String name, QueueContainer container) {
        containerMap.put(name, container);
    }

    // need for testing..
    public boolean containsQueue(String name) {
        return containerMap.containsKey(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, QueueContainer> migrationData = new HashMap<String, QueueContainer>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            QueueContainer container = entry.getValue();

            if (partitionId == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }

        if (migrationData.isEmpty()) {
            return null;
        } else {
            return new QueueReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
        }
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearQueuesHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearQueuesHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearQueuesHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, QueueContainer> entry = iterator.next();
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
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

    @Override
    public void dispatchEvent(QueueEvent event, ItemListener listener) {
        final MemberImpl member = nodeEngine.getClusterService().getMember(event.caller);
        ItemEvent itemEvent = new DataAwareItemEvent(event.name, event.eventType, event.data, member, serializationService);
        if (member == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Dropping event " + itemEvent + " from unknown address:" + event.caller);
            }
            return;
        }

        if (event.eventType.equals(ItemEventType.ADDED)) {
            listener.itemAdded(itemEvent);
        } else {
            listener.itemRemoved(itemEvent);
        }
        getLocalQueueStatsImpl(event.name).incrementReceivedEvents();
    }

    @Override
    public QueueProxyImpl createDistributedObject(String objectId) {
        QueueConfig queueConfig = nodeEngine.getConfig().findQueueConfig(objectId);
        checkQueueConfig(queueConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        return new QueueProxyImpl(objectId, this, nodeEngine, queueConfig);
    }

    @Override
    public void destroyDistributedObject(String name) {
        QueueContainer container = containerMap.remove(name);
        if (container != null) {
            container.destroy();
        }
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
        splitBrainProtectionConfigCache.remove(name);
    }

    public String addItemListener(String name, ItemListener listener, boolean includeValue, boolean isLocal) {
        EventService eventService = nodeEngine.getEventService();
        QueueEventFilter filter = new QueueEventFilter(includeValue);
        EventRegistration registration;
        if (isLocal) {
            registration = eventService.registerLocalListener(
                    QueueService.SERVICE_NAME, name, filter, listener);

        } else {
            registration = eventService.registerListener(
                    QueueService.SERVICE_NAME, name, filter, listener);

        }
        return registration.getId();
    }

    public boolean removeItemListener(String name, String registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, name, registrationId);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * Returns the local queue statistics for the given name and
     * partition ID. If this node is the owner for the partition,
     * returned stats contain {@link LocalQueueStats#getOwnedItemCount()},
     * otherwise it contains {@link LocalQueueStats#getBackupItemCount()}.
     *
     * @param name        the name of the queue for which the statistics are returned
     * @param partitionId the partition ID for which the statistics are returned
     * @return the statistics
     */
    public LocalQueueStats createLocalQueueStats(String name, int partitionId) {
        LocalQueueStatsImpl stats = getLocalQueueStatsImpl(name);
        stats.setOwnedItemCount(0);
        stats.setBackupItemCount(0);
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            return stats;
        }

        Address thisAddress = nodeEngine.getClusterService().getThisAddress();
        IPartition partition = partitionService.getPartition(partitionId, false);

        Address owner = partition.getOwnerOrNull();
        if (thisAddress.equals(owner)) {
            stats.setOwnedItemCount(container.size());
        } else if (owner != null) {
            stats.setBackupItemCount(container.backupSize());
        }
        container.setStats(stats);
        return stats;
    }

    /**
     * Returns the local queue statistics for the queue with the given {@code name}. If this node is the owner of the queue,
     * returned stats contain {@link LocalQueueStats#getOwnedItemCount()}, otherwise it contains
     * {@link LocalQueueStats#getBackupItemCount()}.
     *
     * @param name the name of the queue for which the statistics are returned
     * @return the statistics
     */
    public LocalQueueStats createLocalQueueStats(String name) {
        return createLocalQueueStats(name, getPartitionId(name));
    }

    public LocalQueueStatsImpl getLocalQueueStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localQueueStatsConstructorFunction);
    }

    @Override
    public TransactionalQueueProxy createTransactionalObject(String name, Transaction transaction) {
        return new TransactionalQueueProxy(nodeEngine, this, name, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        final Set<String> queueNames = containerMap.keySet();
        OperationService operationService = nodeEngine.getOperationService();
        for (String name : queueNames) {
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new QueueTransactionRollbackOperation(name, transactionId)
                    .setPartitionId(partitionId)
                    .setService(this)
                    .setNodeEngine(nodeEngine);
            operationService.invokeOnPartition(operation);
        }
    }

    @Override
    public Map<String, LocalQueueStats> getStats() {
        Map<String, LocalQueueStats> queueStats = createHashMap(containerMap.size());
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            LocalQueueStats queueStat = createLocalQueueStats(name);
            queueStats.put(name, queueStat);
        }
        return queueStats;
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        QueueContainerCollector collector = new QueueContainerCollector(nodeEngine, containerMap);
        collector.run();
        return new Merger(collector);
    }

    private int getPartitionId(String name) {
        Data keyData = serializationService.toData(name, StringPartitioningStrategy.INSTANCE);
        return partitionService.getPartitionId(keyData);
    }

    private class Merger extends AbstractContainerMerger<QueueContainer, Collection<Object>, QueueMergeTypes> {

        Merger(QueueContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "queue";
        }

        @Override
        public void runInternal() {
            for (Entry<Integer, Collection<QueueContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<QueueContainer> containerList = entry.getValue();
                for (QueueContainer container : containerList) {
                    // TODO: add batching (which is a bit complex, since collections don't have a multi-name operation yet
                    Queue<QueueItem> items = container.getItemQueue();

                    String name = container.getName();
                    SplitBrainMergePolicy<Collection<Object>, QueueMergeTypes> mergePolicy
                            = getMergePolicy(container.getConfig().getMergePolicyConfig());

                    QueueMergeTypes mergingValue = createMergingValue(serializationService, items);
                    sendBatch(partitionId, name, mergePolicy, mergingValue);
                }
            }
        }

        private void sendBatch(int partitionId, String name,
                               SplitBrainMergePolicy<Collection<Object>, QueueMergeTypes> mergePolicy,
                               QueueMergeTypes mergingValue) {
            QueueMergeOperation operation = new QueueMergeOperation(name, mergePolicy, mergingValue);
            invoke(SERVICE_NAME, operation, partitionId);
        }
    }
}

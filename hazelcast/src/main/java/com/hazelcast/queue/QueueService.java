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

package com.hazelcast.queue;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.queue.proxy.QueueProxyImpl;
import com.hazelcast.queue.tx.QueueTransactionRollbackOperation;
import com.hazelcast.queue.tx.TransactionalQueueProxy;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:21 AM
 */
public class QueueService implements ManagedService, MigrationAwareService, TransactionalService,
        RemoteService, EventPublishingService<QueueEvent, ItemListener> {

    public static final String SERVICE_NAME = "hz:impl:queueService";

    private final NodeEngine nodeEngine;
    private final ConcurrentMap<String, QueueContainer> containerMap = new ConcurrentHashMap<String, QueueContainer>();
    private final ConcurrentMap<String, LocalQueueStatsImpl> statsMap = new ConcurrentHashMap<String, LocalQueueStatsImpl>(1000);
    private final ConstructorFunction<String, LocalQueueStatsImpl> localQueueStatsConstructorFunction = new ConstructorFunction<String, LocalQueueStatsImpl>() {
        public LocalQueueStatsImpl createNew(String key) {
            return new LocalQueueStatsImpl();
        }
    };
    final EntryTaskScheduler queueEvictionScheduler;

    private final ILogger logger;

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        ScheduledExecutorService defaultScheduledExecutor
                = nodeEngine.getExecutionService().getDefaultScheduledExecutor();
        QueueEvictionProcessor entryProcessor = new QueueEvictionProcessor(nodeEngine, this);
        this.queueEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(
                defaultScheduledExecutor, entryProcessor, ScheduleType.POSTPONE);
        this.logger = nodeEngine.getLogger(QueueService.class);
    }

    public void scheduleEviction(String name, long delay) {
        queueEvictionScheduler.schedule(delay, name, null);
    }

    public void cancelEviction(String name) {
        queueEvictionScheduler.cancel(name);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        containerMap.clear();
    }

    public void shutdown(boolean terminate) {
        reset();
    }

    public QueueContainer getOrCreateContainer(final String name, boolean fromBackup) throws Exception {
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new QueueContainer(name, nodeEngine.getConfig().findQueueConfig(name), nodeEngine, this);

            QueueContainer existing = containerMap.putIfAbsent(name, container);
            if (existing != null) {
                container = existing;
            } else {
                container.init(fromBackup);
            }
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

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, QueueContainer> migrationData = new HashMap<String, QueueContainer>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            QueueContainer container = entry.getValue();
            if (partitionId == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }
        return migrationData.isEmpty() ? null : new QueueReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
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

    private void clearMigrationData(int partitionId) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            final Entry<String, QueueContainer> entry = iterator.next();
            final String name = entry.getKey();
            final QueueContainer container = entry.getValue();
            int containerPartitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            if (containerPartitionId == partitionId) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    public void dispatchEvent(QueueEvent event, ItemListener listener) {
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
        getLocalQueueStatsImpl(event.name).incrementReceivedEvents();
    }

    public QueueProxyImpl createDistributedObject(String objectId) {
        return new QueueProxyImpl(objectId, this, nodeEngine);
    }

    public void destroyDistributedObject(String name) {
        containerMap.remove(name);
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
    }

    public String addItemListener(String name, ItemListener listener, boolean includeValue) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(QueueService.SERVICE_NAME, name, new QueueEventFilter(includeValue), listener);
        return registration.getId();
    }

    public boolean removeItemListener(String name, String registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, name, registrationId);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public LocalQueueStats createLocalQueueStats(String name, int partitionId) {
        LocalQueueStatsImpl stats = getLocalQueueStatsImpl(name);
        stats.setOwnedItemCount(0);
        stats.setBackupItemCount(0);
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            return stats;
        }

        Address thisAddress = nodeEngine.getClusterService().getThisAddress();
        InternalPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);

        Address owner = partition.getOwner();
        if (owner == null) {
            //no-op because the owner is not yet set.
        } else if (thisAddress.equals(owner)) {
            stats.setOwnedItemCount(container.size());
        } else {
            stats.setBackupItemCount(container.backupSize());
        }
        container.setStats(stats);
        return stats;
    }

    public LocalQueueStatsImpl getLocalQueueStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localQueueStatsConstructorFunction);
    }

    public TransactionalQueueProxy createTransactionalObject(String name, TransactionSupport transaction) {
        return new TransactionalQueueProxy(nodeEngine, this, name, transaction);
    }

    public void rollbackTransaction(String transactionId) {
        final Set<String> queueNames = containerMap.keySet();
        for (String name : queueNames) {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new QueueTransactionRollbackOperation(name, transactionId).setPartitionId(partitionId).setService(this).setNodeEngine(nodeEngine);
            nodeEngine.getOperationService().executeOperation(operation);
        }
    }
}

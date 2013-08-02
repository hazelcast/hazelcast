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
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.queue.proxy.QueueProxyImpl;
import com.hazelcast.queue.tx.TransactionalQueueProxy;
import com.hazelcast.spi.*;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    private final ILogger logger;
    private final ConstructorFunction<String, LocalQueueStatsImpl> localQueueStatsConstructorFunction = new ConstructorFunction<String, LocalQueueStatsImpl>() {
        public LocalQueueStatsImpl createNew(String key) {
            return new LocalQueueStatsImpl();
        }
    };
    final EntryTaskScheduler queueEvictionScheduler;

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(QueueService.class);
        queueEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(), new QueueEvictionProcessor(nodeEngine, this), true);
    }

    public void scheduleEviction(String name, long delay){
        queueEvictionScheduler.schedule(delay, name, null);
    }

    public void cancelEviction(String name){
        queueEvictionScheduler.cancel(name);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        containerMap.clear();
    }

    public void shutdown() {
        reset();
    }

    public QueueContainer getOrCreateContainer(final String name, boolean fromBackup) throws Exception {
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new QueueContainer(name, nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name)), nodeEngine.getConfig().getQueueConfig(name),
                    nodeEngine, this);
            QueueContainer existing = containerMap.putIfAbsent(name, container);
            if (existing != null) {
                container = existing;
            } else {
                container.init(fromBackup);
            }
        }
        container.cancelEvictionIfExists();
        return container;
    }

    public void addContainer(String name, QueueContainer container) {
        containerMap.put(name, container);
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, QueueContainer> migrationData = new HashMap<String, QueueContainer>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
            if (container.getPartitionId() == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
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
            QueueContainer container = iterator.next().getValue();
            if (container.getPartitionId() == partitionId) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    public void dispatchEvent(QueueEvent event, ItemListener listener) {
        ItemEvent itemEvent = new ItemEvent(event.name, event.eventType, nodeEngine.toObject(event.data),
                nodeEngine.getClusterService().getMember(event.caller));
        if (event.eventType.equals(ItemEventType.ADDED)) {
            listener.itemAdded(itemEvent);
        } else {
            listener.itemRemoved(itemEvent);
        }
        getLocalQueueStatsImpl(event.name).incrementReceivedEvents();
    }

    public QueueProxyImpl createDistributedObject(Object objectId) {
        return new QueueProxyImpl(String.valueOf(objectId), this, nodeEngine);
    }

    public void destroyDistributedObject(Object objectId) {
        final String name = String.valueOf(objectId);
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
        PartitionView partition = nodeEngine.getPartitionService().getPartition(partitionId);
        if (thisAddress.equals(partition.getOwner())) {
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

    public TransactionalQueueProxy createTransactionalObject(Object id, TransactionSupport transaction) {
        return new TransactionalQueueProxy(nodeEngine, this, String.valueOf(id), transaction);
    }
}

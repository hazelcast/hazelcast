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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.queue.proxy.DataQueueProxy;
import com.hazelcast.queue.proxy.ObjectQueueProxy;
import com.hazelcast.spi.*;

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
public class QueueService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService, EventPublishingService<QueueEvent, ItemListener> {

    private NodeEngine nodeEngine;

    public static final String QUEUE_SERVICE_NAME = "hz:impl:queueService";

    private final ConcurrentMap<String, QueueContainer> containerMap = new ConcurrentHashMap<String, QueueContainer>();
    private final ConcurrentMap<ListenerKey, String> eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public QueueContainer getContainer(final String name, boolean fromBackup) throws Exception {
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new QueueContainer(nodeEngine.getPartitionId(nodeEngine.toData(name)), nodeEngine.getConfig().getQueueConfig(name),
                    nodeEngine.getSerializationService(), fromBackup);
            QueueContainer existing = containerMap.putIfAbsent(name, container);
            if (existing != null) {
                container = existing;
            }
        }
        return container;
    }

    public void addContainer(String name, QueueContainer container) {
        containerMap.put(name, container);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        if (event.getPartitionId() < 0 || event.getPartitionId() >= nodeEngine.getPartitionCount()) {
            return null; // is it possible
        }
        Map<String, QueueContainer> migrationData = new HashMap<String, QueueContainer>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
            if (container.getPartitionId() == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }
        return new QueueMigrationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE){
            if (event.getMigrationType() == MigrationType.MOVE || event.getMigrationType() == MigrationType.MOVE_COPY_BACK){
                cleanMigrationData(event.getPartitionId(), event.getCopyBackReplicaIndex());
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            cleanMigrationData(event.getPartitionId(), -1);
        }
    }

    public int getMaxBackupCount() {
        int max = 0;
        for (QueueContainer container : containerMap.values()) {
            int c = container.getConfig().getTotalBackupCount();
            max = Math.max(max,  c);
        }
        return max;
    }

    private void cleanMigrationData(int partitionId, int copyBack) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            QueueContainer container = iterator.next().getValue();
            if (container.getPartitionId() == partitionId && (copyBack ==-1 || container.getConfig().getTotalBackupCount() < copyBack)) {
                iterator.remove();
            }
        }
    }

    public void memberAdded(MembershipServiceEvent membershipEvent) {
    }

    public void memberRemoved(MembershipServiceEvent membershipEvent) {
    }

    public void dispatchEvent(QueueEvent event, ItemListener listener) {
        ItemEvent itemEvent = new ItemEvent(event.name, event.eventType, nodeEngine.toObject(event.data),
                nodeEngine.getCluster().getMember(event.caller));
        if (event.eventType.equals(ItemEventType.ADDED)){
            listener.itemAdded(itemEvent);
        }
        else {
            listener.itemRemoved(itemEvent);
        }
    }

    public String getServiceName() {
        return QUEUE_SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new ObjectQueueProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return new DataQueueProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public void destroyDistributedObject(Object objectId) {

    }

    public void addItemListener(String name, ItemListener listener, boolean includeValue){
        ListenerKey listenerKey = new ListenerKey(listener, name);
        String id = eventRegistrations.putIfAbsent(listenerKey, "tempId");
        if (id != null){
            return;
        }
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(QueueService.QUEUE_SERVICE_NAME, name, new QueueEventFilter(includeValue), listener);
        eventRegistrations.put(listenerKey, registration.getId());
    }

    public void removeItemListener(String name, ItemListener listener){
        ListenerKey listenerKey = new ListenerKey(listener, name);
        String id = eventRegistrations.remove(listenerKey);
        if (id != null){
            EventService eventService = nodeEngine.getEventService();
            eventService.deregisterListener(QUEUE_SERVICE_NAME, name, id);
        }
    }

}

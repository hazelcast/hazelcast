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

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.queue.client.*;
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
public class QueueService implements ManagedService, MigrationAwareService,
        RemoteService, EventPublishingService<QueueEvent, ItemListener>, ClientProtocolService {

    public static final String SERVICE_NAME = "hz:impl:queueService";

    private final NodeEngine nodeEngine;
    private final ConcurrentMap<String, QueueContainer> containerMap = new ConcurrentHashMap<String, QueueContainer>();
    private final ConcurrentMap<ListenerKey, String> eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();
    private final Map<Command, ClientCommandHandler> commandHandlers = new HashMap<Command, ClientCommandHandler>();

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        registerClientOperationHandlers();
    }

    public QueueContainer getContainer(final String name, boolean fromBackup) throws Exception {
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new QueueContainer(nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name)), nodeEngine.getConfig().getQueueConfig(name),
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

    private void registerClientOperationHandlers() {
        commandHandlers.put(Command.QOFFER, new QueueOfferHandler(this));
        commandHandlers.put(Command.QPUT, new QueueOfferHandler(this));
        commandHandlers.put(Command.QPOLL, new QueuePollHandler(this));
        commandHandlers.put(Command.QTAKE, new QueueOfferHandler(this));
        commandHandlers.put(Command.QSIZE, new QueueSizeHandler(this));
        commandHandlers.put(Command.QPEEK, new QueuePollHandler(this));
        commandHandlers.put(Command.QREMOVE, new QueueRemoveHandler(this));
        commandHandlers.put(Command.QREMCAPACITY, new QueueCapacityHandler(this));
        commandHandlers.put(Command.QENTRIES, new QueueEntriesHandler(this));
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
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
                clearMigrationData(event.getPartitionId(), event.getCopyBackReplicaIndex());
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId(), -1);
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

    private void clearMigrationData(int partitionId, int copyBack) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            QueueContainer container = iterator.next().getValue();
            if (container.getPartitionId() == partitionId && (copyBack ==-1 || container.getConfig().getTotalBackupCount() < copyBack)) {
                iterator.remove();
            }
        }
    }

    public void dispatchEvent(QueueEvent event, ItemListener listener) {
        ItemEvent itemEvent = new ItemEvent(event.name, event.eventType, nodeEngine.toObject(event.data),
                nodeEngine.getClusterService().getMember(event.caller));
        if (event.eventType.equals(ItemEventType.ADDED)){
            listener.itemAdded(itemEvent);
        }
        else {
            listener.itemRemoved(itemEvent);
        }
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public ObjectQueueProxy createDistributedObject(Object objectId) {
        return new ObjectQueueProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public DataQueueProxy createDistributedObjectForClient(Object objectId) {
        return new DataQueueProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public void destroyDistributedObject(Object objectId) {
        final String name = String.valueOf(objectId);
        containerMap.remove(name);
        nodeEngine.getEventService().deregisterListeners(SERVICE_NAME, name);
    }

    public void addItemListener(String name, ItemListener listener, boolean includeValue){
        ListenerKey listenerKey = new ListenerKey(listener, name);
        String id = eventRegistrations.putIfAbsent(listenerKey, "tempId");
        if (id != null){
            return;
        }
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(QueueService.SERVICE_NAME, name, new QueueEventFilter(includeValue), listener);
        eventRegistrations.put(listenerKey, registration.getId());
    }

    public void removeItemListener(String name, ItemListener listener){
        ListenerKey listenerKey = new ListenerKey(listener, name);
        String id = eventRegistrations.remove(listenerKey);
        if (id != null){
            EventService eventService = nodeEngine.getEventService();
            eventService.deregisterListener(SERVICE_NAME, name, id);
        }
    }

    public Map<Command, ClientCommandHandler> getCommandMap() {
        return commandHandlers;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public void destroy() {
        containerMap.clear();
        eventRegistrations.clear();
        commandHandlers.clear();
    }
}

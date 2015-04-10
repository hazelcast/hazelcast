/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedMapContainer;
import com.hazelcast.replicatedmap.impl.record.DataReplicatedMapContainer;
import com.hazelcast.replicatedmap.impl.record.ObjectReplicatedRecordStorage;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapContainer;
import com.hazelcast.replicatedmap.impl.record.ReplicationPublisher;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.EventListener;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the main service implementation to handle replication and manages the backing
 * {@link com.hazelcast.replicatedmap.impl.record.ReplicatedMapContainer}s that actually hold the data
 */
public class ReplicatedMapService
        implements ManagedService, RemoteService, EventPublishingService<Object, Object> {

    /**
     * Public constant for the internal service name of the ReplicatedMapService
     */
    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";

    /**
     * Public constant for the internal name of the replication topic
     */
    public static final String EVENT_TOPIC_NAME = SERVICE_NAME + ".replication";

    private final ConcurrentHashMap<String, ReplicatedMapContainer> replicatedStorages = initReplicatedMapContainerMapping();

    private final ConstructorFunction<String, ReplicatedMapContainer> constructor = buildConstructorFunction();

    private final Config config;
    private final NodeEngine nodeEngine;
    private final EventService eventService;
    private final EventRegistration eventRegistration;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
        this.eventRegistration = eventService.registerListener(SERVICE_NAME, EVENT_TOPIC_NAME, new ReplicationListener());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        // Nothing to do, it is ok for a ReplicatedMap to keep its current state on rejoins
    }

    @Override
    public void shutdown(boolean terminate) {
        for (ReplicatedMapContainer replicatedMapContainer : replicatedStorages.values()) {
            replicatedMapContainer.destroy();
        }
        replicatedStorages.clear();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        ReplicatedMapContainer replicatedMapContainer = ConcurrencyUtil
                .getOrPutSynchronized(replicatedStorages, objectName, replicatedStorages, constructor);
        return new ReplicatedMapProxy(nodeEngine, (AbstractReplicatedMapContainer) replicatedMapContainer);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        ReplicatedMapContainer replicatedMapContainer = replicatedStorages.remove(objectName);
        if (replicatedMapContainer != null) {
            replicatedMapContainer.destroy();
        }
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        if (event instanceof EntryEvent) {
            EntryEvent entryEvent = (EntryEvent) event;
            EntryListener entryListener = (EntryListener) listener;
            switch (entryEvent.getEventType()) {
                case ADDED:
                    entryListener.entryAdded(entryEvent);
                    break;
                case EVICTED:
                    entryListener.entryEvicted(entryEvent);
                    break;
                case UPDATED:
                    entryListener.entryUpdated(entryEvent);
                    break;
                case REMOVED:
                    entryListener.entryRemoved(entryEvent);
                    break;
                // TODO handle evictAll and clearAll event
                default:
                    throw new IllegalArgumentException("event type " + entryEvent.getEventType() + " not supported");
            }
            String mapName = ((EntryEvent) event).getName();
            if (config.findReplicatedMapConfig(mapName).isStatisticsEnabled()) {
                ReplicatedMapContainer replicatedMapContainer = replicatedStorages.get(mapName);
                if (replicatedMapContainer instanceof AbstractReplicatedMapContainer) {
                    LocalReplicatedMapStatsImpl stats =
                            ((AbstractReplicatedMapContainer) replicatedMapContainer).getReplicatedMapStats();
                    stats.incrementReceivedEvents();
                }
            }
        } else if (listener instanceof ReplicatedMessageListener) {
            ((ReplicatedMessageListener) listener).onMessage((IdentifiedDataSerializable) event);
        }
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.getReplicatedMapConfig(name).getAsReadOnly();
    }

    public ReplicatedMapContainer getReplicatedMapContainer(String name, boolean create) {
        if (create) {
            return ConcurrencyUtil.getOrPutSynchronized(replicatedStorages, name, replicatedStorages, constructor);
        }
        return replicatedStorages.get(name);
    }

    public String addEventListener(EventListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, eventFilter, entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(String mapName, String registrationId) {
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    private ConcurrentHashMap<String, ReplicatedMapContainer> initReplicatedMapContainerMapping() {
        return new ConcurrentHashMap<String, ReplicatedMapContainer>();
    }

    private ConstructorFunction<String, ReplicatedMapContainer> buildConstructorFunction() {
        return new ConstructorFunction<String, ReplicatedMapContainer>() {

            @Override
            public ReplicatedMapContainer createNew(String name) {
                ReplicatedMapConfig replicatedMapConfig = getReplicatedMapConfig(name);
                InMemoryFormat inMemoryFormat = replicatedMapConfig.getInMemoryFormat();
                AbstractReplicatedMapContainer replicatedRecordStorage = null;
                switch (inMemoryFormat) {
                    case OBJECT:
                        replicatedRecordStorage = new ObjectReplicatedRecordStorage(name, nodeEngine,
                                ReplicatedMapService.this);
                        break;
                    case BINARY:
                        replicatedRecordStorage = new DataReplicatedMapContainer(name, nodeEngine,
                                ReplicatedMapService.this);
                        break;
                    case NATIVE:
                        throw new IllegalStateException("native memory not yet supported for replicated map");
                    default:
                        throw new IllegalStateException("Unhandled in memory format:" + inMemoryFormat);
                }
                return replicatedRecordStorage;
            }
        };
    }

    /**
     * Listener implementation to listen on replication messages from other nodes
     */
    private final class ReplicationListener
            implements ReplicatedMessageListener {

        public void onMessage(IdentifiedDataSerializable message) {
            if (message instanceof ReplicationMessage) {
                ReplicationMessage replicationMessage = (ReplicationMessage) message;
                ReplicatedMapContainer replicatedRecordStorage = replicatedStorages.get(replicationMessage.getName());
                ReplicationPublisher replicationPublisher = replicatedRecordStorage.getReplicationPublisher();
                if (replicatedRecordStorage instanceof AbstractReplicatedMapContainer) {
                    replicationPublisher.queueUpdateMessage(replicationMessage);
                }
            } else if (message instanceof MultiReplicationMessage) {
                MultiReplicationMessage multiReplicationMessage = (MultiReplicationMessage) message;
                ReplicatedMapContainer replicatedRecordStorage = replicatedStorages.get(multiReplicationMessage.getName());
                ReplicationPublisher replicationPublisher = replicatedRecordStorage.getReplicationPublisher();
                if (replicatedRecordStorage instanceof AbstractReplicatedMapContainer) {
                    replicationPublisher.queueUpdateMessages(multiReplicationMessage);
                }
            }
        }
    }
}

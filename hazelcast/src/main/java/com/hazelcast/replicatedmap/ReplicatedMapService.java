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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.record.*;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.NamedThreadFactory;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.*;

public class ReplicatedMapService implements ManagedService, RemoteService,
        PostJoinAwareService, SplitBrainHandlerService,
        EventPublishingService<ReplicationMessage, ReplicatedMessageListener>{

    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";
    public static final String EVENT_TOPIC_NAME = SERVICE_NAME + ".replication";

    private final ConcurrentHashMap<String, ReplicatedRecordStore> replicatedStorages = new ConcurrentHashMap<String, ReplicatedRecordStore>();

    private final CleanerRegistrator cleanerRegistrator = new CleanerRegistrator() {
        @Override
        public <V> ScheduledFuture<V> registerCleaner(AbstractReplicatedRecordStore replicatedRecordStorage) {
            return (ScheduledFuture) ReplicatedMapService.this.registerCleaner(replicatedRecordStorage);
        }
    };

    private final ConstructorFunction<String, ReplicatedRecordStore> constructor = new ConstructorFunction<String, ReplicatedRecordStore>() {
        @Override
        public ReplicatedRecordStore createNew(String name) {
            ReplicatedMapConfig replicatedMapConfig = getReplicatedMapConfig(name);
            InMemoryFormat inMemoryFormat = replicatedMapConfig.getInMemoryFormat();
            AbstractReplicatedRecordStore replicatedRecordStorage = null;
            switch (inMemoryFormat) {
                case OBJECT:
                    replicatedRecordStorage = new ObjectReplicatedRecordStorage(
                            name, nodeEngine, cleanerRegistrator, ReplicatedMapService.this);
                    break;
                case BINARY:
                    replicatedRecordStorage = new DataReplicatedRecordStore(
                            name, nodeEngine, cleanerRegistrator, ReplicatedMapService.this);
                    break;
            }
            if (replicatedRecordStorage == null) {
                //TODO
                throw new InvalidStateException("offheap not yet supported for replicated map");
            }
            return replicatedRecordStorage;
        }
    };

    private final ScheduledExecutorService cleanerExecutorService;

    private final ILogger logger;
    private final Config config;
    private final NodeEngine nodeEngine;
    private final EventService eventService;

    private final EventRegistration eventRegistration;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(ReplicatedMapService.class.getName());
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
        this.cleanerExecutorService = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(SERVICE_NAME + ".cleaner"));
        this.eventRegistration = eventService.registerListener(
                SERVICE_NAME, EVENT_TOPIC_NAME, new ReplicationListener());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        for (ReplicatedRecordStore replicatedRecordStore : replicatedStorages.values()) {
            replicatedRecordStore.clear();
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        for (ReplicatedRecordStore replicatedRecordStore : replicatedStorages.values()) {
            replicatedRecordStore.destroy();
        }
        replicatedStorages.clear();
    }

    @Override
    public Operation getPostJoinOperation() {
        return null;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        ReplicatedRecordStore replicatedRecordStore = ConcurrencyUtil
                .getOrPutSynchronized(replicatedStorages, objectName, replicatedStorages, constructor);
        return new ReplicatedMapProxy(nodeEngine, replicatedRecordStore);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        ReplicatedRecordStore replicatedRecordStore = replicatedStorages.remove(objectName);
        if (replicatedRecordStore != null) {
            replicatedRecordStore.destroy();
        }
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return null;
    }

    @Override
    public void dispatchEvent(ReplicationMessage event, ReplicatedMessageListener listener) {
        listener.onMessage(event);
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.getReplicatedMapConfig(name).getAsReadOnly();
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name) {
        return replicatedStorages.get(name);
    }

    ScheduledFuture<?> registerCleaner(AbstractReplicatedRecordStore replicatedRecordStorage) {
        return cleanerExecutorService.scheduleWithFixedDelay(
                new Cleaner(replicatedRecordStorage), 5, 5, TimeUnit.SECONDS);
    }

    private class ReplicationListener implements ReplicatedMessageListener {

        public void onMessage(IdentifiedDataSerializable message) {
            if (message instanceof ReplicationMessage) {
                ReplicationMessage replicationMessage = (ReplicationMessage) message;
                ReplicatedRecordStore replicatedRecordStorage = replicatedStorages.get(replicationMessage.getName());
                if (replicatedRecordStorage instanceof AbstractReplicatedRecordStore) {
                    ((AbstractReplicatedRecordStore) replicatedRecordStorage).queueUpdateMessage(replicationMessage);
                }
            } else if (message instanceof MultiReplicationMessage) {
                MultiReplicationMessage multiReplicationMessage = (MultiReplicationMessage) message;
                ReplicatedRecordStore replicatedRecordStorage = replicatedStorages.get(multiReplicationMessage.getName());
                for (ReplicationMessage replicationMessage : multiReplicationMessage.getReplicationMessages()) {
                    if (replicatedRecordStorage instanceof AbstractReplicatedRecordStore) {
                        ((AbstractReplicatedRecordStore) replicatedRecordStorage).queueUpdateMessage(replicationMessage);
                    }
                }
            }
        }
    }

    private static class Cleaner implements Runnable {

        private final long ttl = TimeUnit.SECONDS.toMillis(10);
        private final AbstractReplicatedRecordStore replicatedRecordStorage;

        private Cleaner(AbstractReplicatedRecordStore replicatedRecordStorage) {
            this.replicatedRecordStorage = replicatedRecordStorage;
        }

        public void run() {
            final Iterator<ReplicatedRecord> iterator = replicatedRecordStorage.getRecords().iterator();
            final long now = System.currentTimeMillis();
            while (iterator.hasNext()) {
                final ReplicatedRecord v = iterator.next();
                if (v.getValue() == null && (v.getUpdateTime() + ttl) < now) {
                    iterator.remove();
                }
            }
        }
    }

}

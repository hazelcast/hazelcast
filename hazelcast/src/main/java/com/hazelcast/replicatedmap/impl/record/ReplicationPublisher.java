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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.ReplicationChannel;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class implements the actual replication logic for replicated map
 *
 * @param <K>
 * @param <V>
 */
public class ReplicationPublisher<K, V> implements ReplicationChannel {
    private static final ILogger LOGGER = Logger.getLogger(ReplicationPublisher.class);

    private static final String SERVICE_NAME = ReplicatedMapService.SERVICE_NAME;
    private static final String EVENT_TOPIC_NAME = ReplicatedMapService.EVENT_TOPIC_NAME;

    private static final int MAX_MESSAGE_CACHE_SIZE = 1000;

    private final List<ReplicationMessage> replicationMessageCache = new ArrayList<ReplicationMessage>();
    private final Lock replicationMessageCacheLock = new ReentrantLock();

    private final ScheduledExecutorService executorService;
    private final EventService eventService;
    private final InternalPartitionService partitionService;
    private final NodeEngine nodeEngine;

    private final AbstractBaseReplicatedRecordStore<K, V> replicatedRecordStore;
    private final InternalReplicatedMapStorage<K, V> storage;
    private final ReplicatedMapConfig replicatedMapConfig;
    private final LocalReplicatedMapStatsImpl mapStats;
    private final Member localMember;
    private final String name;


    ReplicationPublisher(AbstractBaseReplicatedRecordStore<K, V> replicatedRecordStore, NodeEngine nodeEngine) {
        this.replicatedRecordStore = replicatedRecordStore;
        this.nodeEngine = nodeEngine;
        this.name = replicatedRecordStore.getName();
        this.storage = replicatedRecordStore.storage;
        this.mapStats = replicatedRecordStore.mapStats;
        this.eventService = nodeEngine.getEventService();
        this.localMember = replicatedRecordStore.localMember;
        this.replicatedMapConfig = replicatedRecordStore.replicatedMapConfig;
        this.executorService = getExecutorService(nodeEngine, replicatedMapConfig);
        this.partitionService = nodeEngine.getPartitionService();
    }

    @Override
    public void replicate(ReplicationMessage message) {
        distributeReplicationMessage(message);
    }

    public void publishReplicatedMessage(ReplicationMessage message) {
        if (replicatedMapConfig.getReplicationDelayMillis() == 0) {
            distributeReplicationMessage(message);
        } else {
            replicationMessageCacheLock.lock();
            try {
                replicationMessageCache.add(message);
                if (replicationMessageCache.size() == 1) {
                    ReplicationCachedSenderTask task = new ReplicationCachedSenderTask(this);
                    long replicationDelayMillis = replicatedMapConfig.getReplicationDelayMillis();
                    executorService.schedule(task, replicationDelayMillis, TimeUnit.MILLISECONDS);
                } else {
                    if (replicationMessageCache.size() > MAX_MESSAGE_CACHE_SIZE) {
                        processMessageCache();
                    }
                }
            } finally {
                replicationMessageCacheLock.unlock();
            }
        }
    }

    public void queueUpdateMessage(final ReplicationMessage update) {
        if (localMember.equals(update.getOrigin())) {
            return;
        }
        mapStats.incrementReceivedReplicationEvents();
        Data dataKey = update.getDataKey();
        Object objectKey = nodeEngine.toObject(dataKey);
        if (objectKey instanceof String) {
            String key = (String) objectKey;
            if (AbstractReplicatedRecordStore.CLEAR_REPLICATION_MAGIC_KEY.equals(key)) {
                storage.clear();
                return;
            }
        }

        K marshalledKey = (K) replicatedRecordStore.marshallKey(update.getDataKey());
        final ReplicatedRecord<K, V> localEntry = storage.get(marshalledKey);
        if (localEntry == null) {
            createLocalEntry(update, marshalledKey);
        } else {
            updateLocalEntry(update, localEntry);
        }
    }

    void destroy() {
        executorService.shutdownNow();
    }

    void processMessageCache() {
        ReplicationMessage[] replicationMessages = null;
        replicationMessageCacheLock.lock();
        try {
            final int size = replicationMessageCache.size();
            if (size > 0) {
                replicationMessages = replicationMessageCache.toArray(new ReplicationMessage[size]);
                replicationMessageCache.clear();
            }
        } finally {
            replicationMessageCacheLock.unlock();
        }
        if (replicationMessages != null) {
            for (int i = 0; i < replicationMessages.length; i++) {
                distributeReplicationMessage(replicationMessages[i]);
            }
        }
    }

    void distributeReplicationMessage(final Object message) {
        Collection<EventRegistration> eventRegistrations = eventService.getRegistrations(SERVICE_NAME, EVENT_TOPIC_NAME);
        Collection<EventRegistration> registrations = filterEventRegistrations(eventRegistrations);
        eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registrations, message, name.hashCode());
    }


    public void emptyReplicationQueue() {
        replicationMessageCacheLock.lock();
        try {
            replicationMessageCache.clear();
        } finally {
            replicationMessageCacheLock.unlock();
        }
    }

    private void createLocalEntry(ReplicationMessage update, K marshalledKey) {
        V marshalledValue = (V) replicatedRecordStore.marshallValue(update.getDataValue());
        int updateHash = update.getUpdateHash();
        long ttlMillis = update.getTtlMillis();

        int partitionId = partitionService.getPartitionId(marshalledKey);
        storage.put(marshalledKey,
                new ReplicatedRecord<K, V>(marshalledKey, marshalledValue, updateHash, ttlMillis, partitionId));
        if (ttlMillis > 0) {
            replicatedRecordStore.scheduleTtlEntry(ttlMillis, marshalledKey, marshalledValue);
        } else {
            replicatedRecordStore.cancelTtlEntry(marshalledKey);
        }
        replicatedRecordStore.fireEntryListenerEvent(update.getDataKey(), null, update.getDataValue());
    }

    private void updateLocalEntry(ReplicationMessage update, ReplicatedRecord<K, V> localEntry) {
        K marshalledKey = (K) replicatedRecordStore.marshallKey(update.getDataKey());
        V marshalledValue = (V) replicatedRecordStore.marshallValue(update.getDataValue());
        long ttlMillis = update.getTtlMillis();
        long oldTtlMillis = localEntry.getTtlMillis();
        Object oldValue = localEntry.setValueInternal(marshalledValue, update.getUpdateHash(), ttlMillis);

        if (ttlMillis > 0 || update.isRemove()) {
            replicatedRecordStore.scheduleTtlEntry(ttlMillis, marshalledKey, null);
        } else {
            replicatedRecordStore.cancelTtlEntry(marshalledKey);
        }

        V unmarshalledOldValue = (V) replicatedRecordStore.unmarshallValue(oldValue);
        if (unmarshalledOldValue == null || !unmarshalledOldValue.equals(update.getDataValue())
                || update.getTtlMillis() != oldTtlMillis) {

            replicatedRecordStore.fireEntryListenerEvent(update.getDataKey(), unmarshalledOldValue, update.getDataValue());
        }
    }

    private Collection<EventRegistration> filterEventRegistrations(Collection<EventRegistration> eventRegistrations) {
        Address address = localMember.getAddress();
        List<EventRegistration> registrations = new ArrayList<EventRegistration>(eventRegistrations);
        Iterator<EventRegistration> iterator = registrations.iterator();
        while (iterator.hasNext()) {
            EventRegistration registration = iterator.next();
            if (address.equals(registration.getSubscriber())) {
                iterator.remove();
            }
        }
        return registrations;
    }

    private ScheduledExecutorService getExecutorService(NodeEngine nodeEngine, ReplicatedMapConfig replicatedMapConfig) {
        ScheduledExecutorService es = replicatedMapConfig.getReplicatorExecutorService();
        if (es == null) {
            es = nodeEngine.getExecutionService().getDefaultScheduledExecutor();
        }
        return new WrappedExecutorService(es);
    }
}

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
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEvictionProcessor;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.util.scheduler.ScheduledEntry;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;

/**
 * Internal base class to encapsulate the internals from the interface methods of ReplicatedRecordStore
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class AbstractBaseReplicatedRecordStore<K, V> implements ReplicatedRecordStore, InitializingObject {

    protected final InternalReplicatedMapStorage<K, V> storage;

    protected final ReplicatedMapService replicatedMapService;
    protected final ReplicatedMapConfig replicatedMapConfig;
    protected final NodeEngineImpl nodeEngine;
    protected final SerializationService serializationService;
    protected final InternalPartitionService partitionService;
    protected final AtomicBoolean isLoaded = new AtomicBoolean(false);
    protected final EntryTaskScheduler ttlEvictionScheduler;
    protected final EventService eventService;
    protected final String name;
    protected int partitionId;


    protected AbstractBaseReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = (NodeEngineImpl) replicatedMapService.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.eventService = nodeEngine.getEventService();
        this.replicatedMapService = replicatedMapService;
        this.replicatedMapConfig = replicatedMapService.getReplicatedMapConfig(name);
        this.storage = new InternalReplicatedMapStorage<K, V>();
        this.ttlEvictionScheduler = EntryTaskSchedulerFactory
                .newScheduler(nodeEngine.getExecutionService().getDefaultScheduledExecutor(),
                        new ReplicatedMapEvictionProcessor(nodeEngine, replicatedMapService, name)
                        , ScheduleType.POSTPONE);
    }

    public InternalReplicatedMapStorage<K, V> getStorage() {
        return storage;
    }

    @Override
    public String getName() {
        return name;
    }

    public LocalReplicatedMapStatsImpl getStats() {
        return replicatedMapService.getLocalMapStatsImpl(name);
    }

    @Override
    public void initialize() {
    }

    @Override
    public void destroy() {
        storage.clear();
        replicatedMapService.destroyDistributedObject(getName());
    }


    public long getVersion() {
        return storage.getVersion();
    }

    public void setVersion(long version) {
        long versionDiff = version - storage.getVersion();
        if (versionDiff > 1) {
            System.err.println("VERSION DIFF IS BIGGER THAN 1 , version = " + storage.getVersion() + ", suggested = " + version);
        }
        storage.setVersion(version);
    }

    public ReplicatedMapService getReplicatedMapService() {
        return replicatedMapService;
    }

    public Set<ReplicatedRecord> getRecords() {
        return new HashSet<ReplicatedRecord>(storage.values());
    }


    ScheduledEntry<K, V> cancelTtlEntry(K key) {
        return ttlEvictionScheduler.cancel(key);
    }

    boolean scheduleTtlEntry(long delayMillis, K key, V object) {
        return ttlEvictionScheduler.schedule(delayMillis, key, object);
    }

    void fireEntryListenerEvent(Object key, Object oldValue, Object value) {
        EntryEventType eventType = value == null ? REMOVED : oldValue == null ? ADDED : UPDATED;
        fireEntryListenerEvent(key, oldValue, value, eventType);
    }

    void fireEntryListenerEvent(Object key, Object oldValue, Object value, EntryEventType eventType) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(
                ReplicatedMapService.SERVICE_NAME, name);
        if (registrations.size() <= 0) {
            return;
        }
        Data dataKey = serializationService.toData(key);
        Data dataValue = serializationService.toData(value);
        Data dataOldValue = serializationService.toData(oldValue);
        EntryEventData eventData = new EntryEventData(name, name, nodeEngine.getThisAddress(),
                dataKey, dataValue, dataOldValue, eventType.getType());
        for (EventRegistration registration : registrations) {
            EventFilter filter = registration.getFilter();
            boolean publish = filter == null || filter.eval(dataKey);
            if (publish) {
                eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registration,
                        eventData, dataKey.hashCode());
            }
        }
    }


    @Override
    public boolean isLoaded() {
        return isLoaded.get();
    }

    @Override
    public void setLoaded(boolean loaded) {
        isLoaded.set(loaded);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractBaseReplicatedRecordStore that = (AbstractBaseReplicatedRecordStore) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (!storage.equals(that.storage)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = storage.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}

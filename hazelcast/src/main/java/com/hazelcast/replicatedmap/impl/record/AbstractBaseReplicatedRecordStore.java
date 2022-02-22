/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEvictionProcessor;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.scheduler.EntryTaskScheduler;
import com.hazelcast.internal.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.internal.util.scheduler.ScheduleType;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Internal base class to encapsulate the internals from the interface methods of ReplicatedRecordStore
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class AbstractBaseReplicatedRecordStore<K, V> implements ReplicatedRecordStore {

    protected int partitionId;

    protected final String name;
    protected final NodeEngine nodeEngine;
    protected final EventService eventService;
    protected final IPartitionService partitionService;
    protected final ReplicatedMapConfig replicatedMapConfig;
    protected final SerializationService serializationService;
    protected final ReplicatedMapService replicatedMapService;
    protected final AtomicReference<InternalReplicatedMapStorage<K, V>> storageRef;
    protected final AtomicBoolean isLoaded = new AtomicBoolean(false);

    private final EntryTaskScheduler<Object, Object> ttlEvictionScheduler;

    protected AbstractBaseReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = replicatedMapService.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.eventService = nodeEngine.getEventService();
        this.replicatedMapService = replicatedMapService;
        this.replicatedMapConfig = replicatedMapService.getReplicatedMapConfig(name);
        this.storageRef = new AtomicReference<>();
        this.storageRef.set(new InternalReplicatedMapStorage<>());
        this.ttlEvictionScheduler = EntryTaskSchedulerFactory
                .newScheduler(nodeEngine.getExecutionService().getGlobalTaskScheduler(),
                        new ReplicatedMapEvictionProcessor(this, nodeEngine, partitionId), ScheduleType.POSTPONE);
    }

    public InternalReplicatedMapStorage<K, V> getStorage() {
        return storageRef.get();
    }

    public AtomicReference<InternalReplicatedMapStorage<K, V>> getStorageRef() {
        return storageRef;
    }

    // only used for testing purposes
    public EntryTaskScheduler getTtlEvictionScheduler() {
        return ttlEvictionScheduler;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getName() {
        return name;
    }

    public LocalReplicatedMapStatsImpl getStats() {
        return replicatedMapService.getLocalReplicatedMapStatsImpl(name);
    }

    @Override
    public void destroy() {
        InternalReplicatedMapStorage storage = storageRef.getAndSet(new InternalReplicatedMapStorage<>());
        if (storage != null) {
            storage.clear();
        }
        ttlEvictionScheduler.cancelAll();
    }

    protected InternalReplicatedMapStorage<K, V> clearInternal() {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        storage.clear();
        getStats().incrementOtherOperations();
        ttlEvictionScheduler.cancelAll();
        return storage;
    }

    @Override
    public long getVersion() {
        return storageRef.get().getVersion();
    }

    @Override
    public boolean isStale(long version) {
        return storageRef.get().isStale(version);
    }

    public Set<ReplicatedRecord> getRecords() {
        return new HashSet<>(storageRef.get().values());
    }

    @Override
    public ScheduledEntry<Object, Object> cancelTtlEntry(Object key) {
        return ttlEvictionScheduler.cancel(key);
    }

    @Override
    public boolean scheduleTtlEntry(long delayMillis, Object key, Object value) {
        return ttlEvictionScheduler.schedule(delayMillis, key, value);
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
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        return storageRef.get().equals(that.storageRef.get());
    }

    @Override
    public int hashCode() {
        int result = storageRef.get().hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}

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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.AbstractMapDataStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write behind map data store implementation.
 * Created per every record-store. So only called from one-thread.
 */
public class WriteBehindStore extends AbstractMapDataStore<Data, Object> {

    /**
     * Represents a transient {@link DelayedEntry}.
     * A transient entry can be added via {@link com.hazelcast.core.IMap#putTransient}.
     */
    private static final DelayedEntry TRANSIENT = new DelayedEntry();

    private final long writeDelayTime;

    private final int partitionId;

    /**
     * Count of issued flush operations.
     * It may be caused from an eviction. Instead of directly flushing entries
     * upon eviction, only counting the number of flushes and immediately process them
     * in {@link com.hazelcast.map.impl.mapstore.writebehind.StoreWorker}.
     */
    private final AtomicInteger flushCounter;

    private WriteBehindQueue<DelayedEntry> writeBehindQueue;

    private WriteBehindProcessor writeBehindProcessor;

    /**
     * A temporary living space for evicted data if we are using a write-behind map store.
     * Because every eviction triggers a map store flush and in write-behind mode this flush operation
     * should not cause any inconsistencies like reading a stale value from map store.
     * To prevent reading stale value, when the time of a non-existent key requested, before loading it from map-store
     * first we are searching for an evicted entry in this space and if it is not there,
     * we are asking map store to load it. All read operations will use this staging area
     * to return last set value on a specific key, since there is a possibility that
     * {@link com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue} may contain more than one waiting operations
     * on a specific key.
     * <p/>
     * NOTE: In case of eviction we do not want to make a huge database load by flushing entries uncontrollably.
     * And also does not want to make duplicate map-store calls for a key. This is why we do not use direct flushing option
     * to map-store instead of staging area.
     */
    private final ConcurrentMap<Data, DelayedEntry> stagingArea;


    public WriteBehindStore(MapStoreWrapper store, SerializationService serializationService,
                            long writeDelayTime, int partitionId) {
        super(store, serializationService);
        this.writeDelayTime = writeDelayTime;
        this.partitionId = partitionId;
        this.stagingArea = createStagingArea();
        this.flushCounter = new AtomicInteger(0);
    }

    private ConcurrentHashMap<Data, DelayedEntry> createStagingArea() {
        return new ConcurrentHashMap<Data, DelayedEntry>();
    }

    public void setWriteBehindQueue(WriteBehindQueue<DelayedEntry> writeBehindQueue) {
        this.writeBehindQueue = writeBehindQueue;
    }

    // TODO when mode is not write-coalescing, clone value objects. this is for EntryProcessors in object memory format.
    @Override
    public Object add(Data key, Object value, long now) {
        final long writeDelay = this.writeDelayTime;
        final long storeTime = now + writeDelay;
        final DelayedEntry<Data, Object> delayedEntry =
                DelayedEntry.create(key, value, storeTime, partitionId);

        add(delayedEntry);

        return value;
    }

    public void add(DelayedEntry<Data, Object> delayedEntry) {
        writeBehindQueue.offer(delayedEntry);
        stagingArea.put(delayedEntry.getKey(), delayedEntry);
    }

    @Override
    public void addTransient(Data key, long now) {
        stagingArea.put(key, TRANSIENT);
    }

    @Override
    public Object addBackup(Data key, Object value, long time) {
        return add(key, value, time);
    }

    @Override
    public void remove(Data key, long now) {
        final long writeDelay = this.writeDelayTime;
        final long storeTime = now + writeDelay;
        final DelayedEntry<Data, Object> delayedEntry =
                DelayedEntry.createWithNullValue(key, storeTime, partitionId);

        add(delayedEntry);
    }

    @Override
    public void removeBackup(Data key, long time) {
        remove(key, time);
    }

    @Override
    public void clear() {
        writeBehindQueue.clear();
        stagingArea.clear();
        flushCounter.set(0);
    }

    @Override
    public Object load(Data key) {
        DelayedEntry delayedEntry = getFromStagingArea(key);
        return delayedEntry == null ? getStore().load(toObject(key))
                : toObject(delayedEntry.getValue());
    }

    @Override
    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Object, Object> map = new HashMap<Object, Object>();
        final Iterator iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Object key = iterator.next();
            final Data dataKey = toData(key);

            DelayedEntry delayedEntry = getFromStagingArea(dataKey);
            if (delayedEntry != null) {
                Object value = delayedEntry.getValue();
                if (value != null) {
                    map.put(dataKey, toObject(value));
                }
                iterator.remove();
            }
        }
        map.putAll(super.loadAll(keys));
        return map;
    }

    /**
     * * Used in {@link com.hazelcast.core.IMap#loadAll} calls.
     * If write-behind map-store feature enabled, some things may lead possible data inconsistencies.
     * These are:
     * - calling evict/evictAll.
     * - calling remove.
     * - not yet stored write behind queue operation.
     * <p/>
     * With this method we can be sure that a key can be loadable from map-store or not.
     *
     * @param key            to query whether loadable or not.
     * @param lastUpdateTime last update time.
     * @param now            in mills
     * @return <code>true</code> if loadable, otherwise false.
     */
    @Override
    public boolean loadable(Data key, long lastUpdateTime, long now) {
        if (isInStagingArea(key, now)
                || hasAnyWaitingOperationInWriteBehindQueue(lastUpdateTime, now)) {
            return false;
        }
        return true;
    }

    @Override
    public int notFinishedOperationsCount() {
        return writeBehindQueue.size();
    }

    @Override
    public Object flush(Data key, Object value, long now, boolean backup) {
        DelayedEntry delayedEntry = stagingArea.get(key);
        if (delayedEntry == TRANSIENT) {
            stagingArea.remove(key);
            return null;
        }

        if (writeBehindQueue.size() == 0) {
            return null;
        }

        flushCounter.incrementAndGet();
        return value;
    }

    @Override
    public Collection<Data> flush() {
        return writeBehindProcessor.flush(writeBehindQueue);
    }

    /**
     * Removes processed entries from temporary spaces.
     */
    void removeProcessed(DelayedEntry entry) {
        if (entry == null) {
            return;
        }

        Object key = entry.getKey();
        stagingArea.remove(key, entry);
    }

    private boolean hasAnyWaitingOperationInWriteBehindQueue(long lastUpdateTime, long now) {
        final long scheduledStoreTime = lastUpdateTime + writeDelayTime;
        return now < scheduledStoreTime;
    }

    private boolean isInStagingArea(Data key, long now) {
        final DelayedEntry entry = stagingArea.get(key);
        if (entry == null) {
            return false;
        }
        final long storeTime = entry.getStoreTime();
        return now < storeTime;
    }

    private DelayedEntry getFromStagingArea(Data key) {
        DelayedEntry entry = stagingArea.get(key);
        if (entry == null || entry == TRANSIENT) {
            return null;
        }
        return entry;
    }

    public WriteBehindQueue<DelayedEntry> getWriteBehindQueue() {
        return writeBehindQueue;
    }

    public void setWriteBehindProcessor(WriteBehindProcessor writeBehindProcessor) {
        this.writeBehindProcessor = writeBehindProcessor;
    }

    public AtomicInteger getFlushCounter() {
        return flushCounter;
    }

}

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
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
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
     *
     * @see com.hazelcast.map.impl.mapstore.writebehind.entry.TransientDelayedEntry
     */
    private static final DelayedEntry TRANSIENT = DelayedEntries.emptyDelayedEntry();

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
     * Also this space is used to control any waiting delete operations on a key or any transiently put entries to {@code IMap}.
     * Values of any transiently put entries should not be added to this area upon eviction, otherwise subsequent
     * {@code IMap#get} operations may return stale values.
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
        this.stagingArea = new ConcurrentHashMap<Data, DelayedEntry>();
        this.flushCounter = new AtomicInteger(0);
    }


    // TODO when mode is not write-coalescing, clone value objects. this is for EntryProcessors in object memory format.
    @Override
    public Object add(Data key, Object value, long now) {
        long writeDelay = this.writeDelayTime;
        long storeTime = now + writeDelay;
        DelayedEntry<Data, Object> delayedEntry
                = DelayedEntries.createDefault(key, value, storeTime, partitionId);

        add(delayedEntry);

        return value;
    }

    public void add(DelayedEntry<Data, Object> delayedEntry) {
        writeBehindQueue.addLast(delayedEntry);
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
        long writeDelay = this.writeDelayTime;
        long storeTime = now + writeDelay;
        DelayedEntry<Data, Object> delayedEntry
                = DelayedEntries.createWithoutValue(key, storeTime, partitionId);

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
        if (delayedEntry == null) {
            return getStore().load(toObject(key));
        }
        return toObject(delayedEntry.getValue());
    }

    @Override
    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Object, Object> map = new HashMap<Object, Object>();
        Iterator iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Data dataKey = toData(key);

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
     * @param key to query whether loadable or not.
     * @return <code>true</code> if loadable, otherwise false.
     */
    @Override
    public boolean loadable(Data key) {
        return !writeBehindQueue.contains(DelayedEntries.createDefault(key, null, -1, -1));
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

        if (writeBehindQueue.size() == 0
                || !writeBehindQueue.contains(DelayedEntries.createWithoutValue(key))) {
            return null;
        }
        flushCounter.incrementAndGet();

        return value;
    }

    @Override
    public Collection<Data> flush() {
        return writeBehindProcessor.flush(writeBehindQueue);
    }

    public WriteBehindQueue<DelayedEntry> getWriteBehindQueue() {
        return writeBehindQueue;
    }

    public void setWriteBehindQueue(WriteBehindQueue<DelayedEntry> writeBehindQueue) {
        this.writeBehindQueue = writeBehindQueue;
    }

    public void setWriteBehindProcessor(WriteBehindProcessor writeBehindProcessor) {
        this.writeBehindProcessor = writeBehindProcessor;
    }

    public AtomicInteger getFlushCounter() {
        return flushCounter;
    }

    void removeFromStagingArea(DelayedEntry delayedEntry) {
        if (delayedEntry == null) {
            return;
        }
        Data key = (Data) delayedEntry.getKey();
        stagingArea.remove(key, delayedEntry);
    }

    private DelayedEntry getFromStagingArea(Data key) {
        DelayedEntry delayedEntry = stagingArea.get(key);
        if (delayedEntry == null || delayedEntry == TRANSIENT) {
            return null;
        }
        return delayedEntry;
    }

}

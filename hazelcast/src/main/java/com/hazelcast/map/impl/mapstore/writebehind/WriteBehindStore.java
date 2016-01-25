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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.AbstractMapDataStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;

/**
 * Write behind map data store implementation.
 * Created per every record-store. Only called from one thread.
 */
public class WriteBehindStore extends AbstractMapDataStore<Data, Object> {

    /**
     * Represents a transient {@link DelayedEntry}.
     * A transient entry can be added via {@link com.hazelcast.core.IMap#putTransient}.
     */
    private static final DelayedEntry TRANSIENT = DelayedEntries.emptyDelayedEntry();

    private final long writeDelayTime;

    private final int partitionId;

    /**
     * Number of issued flush operations.
     * Flushes may be caused by an eviction. Instead of directly flushing entries
     * upon eviction, the flushes are counted and immediately processed
     * in {@link com.hazelcast.map.impl.mapstore.writebehind.StoreWorker}.
     */
    private final AtomicInteger flushCounter;

    private final InMemoryFormat inMemoryFormat;

    /**
     * @see {@link com.hazelcast.config.MapStoreConfig#setWriteCoalescing(boolean)}
     */
    private final boolean coalesce;

    private WriteBehindQueue<DelayedEntry> writeBehindQueue;

    private WriteBehindProcessor writeBehindProcessor;

    /**
     * {@code stagingArea} is a temporary living space for evicted data if we are using a write-behind map store.
     * Every eviction triggers a map store flush, and in write-behind mode this flush operation
     * should not cause any inconsistencies, such as reading a stale value from map store.
     * To prevent reading stale values when the time of a non-existent key is requested, before loading it from map-store
     * we search for an evicted entry in this space. If the entry is not there,
     * we ask map store to load it. All read operations use this staging area
     * to return the last set value on a specific key, since there is a possibility that
     * {@link com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue} may contain more than one waiting operations
     * on a specific key.
     * <p/>
     * This space is also used to control any waiting delete operations on a key or any transiently put entries to {@code IMap}.
     * Values of any transiently put entries should not be added to this area upon eviction, otherwise subsequent
     * {@code IMap#get} operations may return stale values.
     * <p/>
     * NOTE: In case of eviction we do not want to make a huge database load by flushing entries uncontrollably.
     * We also do not want to make duplicate map-store calls for a key. This is why we use the staging area instead of the
     * direct flushing option to map-store.
     */
    private final ConcurrentMap<Data, DelayedEntry> stagingArea;

    public WriteBehindStore(MapStoreWrapper store, SerializationService serializationService,
                            long writeDelayTime, int partitionId, InMemoryFormat inMemoryFormat, boolean coalesce) {
        super(store, serializationService);
        this.writeDelayTime = writeDelayTime;
        this.partitionId = partitionId;
        this.stagingArea = new ConcurrentHashMap<Data, DelayedEntry>();
        this.flushCounter = new AtomicInteger(0);
        this.inMemoryFormat = inMemoryFormat;
        this.coalesce = coalesce;
    }


    @Override
    public Object add(Data key, Object value, long now) {

        // When using format InMemoryFormat.NATIVE, just copy key & value to heap.
        if (NATIVE == inMemoryFormat) {
            value = toData(value);
            key = toData(key);
        }

        // This note describes the problem when we want to persist all states of an entry (means write-coalescing is off)
        // by using both EntryProcessor + OBJECT in-memory-format:
        //
        // If in-memory-format is OBJECT, there is a possibility that a previous state of an entry can be overwritten
        // by a subsequent write operation while both are waiting in the write-behind-queue, this is because they are referencing
        // to the same entry-value. To prevent such a problem, we are taking snapshot of the value by serializing it,
        // this means an extra serialization and additional latency for operations like map#put but it is needed,
        // otherwise we can lost a state.
        if (!coalesce && OBJECT == inMemoryFormat) {
            value = toData(value);
        }

        long storeTime = now + writeDelayTime;
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
        if (NATIVE == inMemoryFormat) {
            key = toData(key);
        }

        stagingArea.put(key, TRANSIENT);
    }

    @Override
    public Object addBackup(Data key, Object value, long time) {
        return add(key, value, time);
    }

    @Override
    public void remove(Data key, long now) {
        if (NATIVE == inMemoryFormat) {
            key = toData(key);
        }

        long storeTime = now + writeDelayTime;
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
     * If the write-behind map-store feature is enabled, some things may lead to possible data inconsistencies.
     * These are:
     * - calling evict/evictAll,
     * - calling remove, and
     * - not yet stored write-behind queue operations.
     * <p/>
     * With this method, we can be sure if a key can be loadable from map-store or not.
     *
     * @param key the key to query whether it is loadable or not.
     * @return <code>true</code> if loadable, false otherwise.
     */
    @Override
    public boolean loadable(Data key) {
        if (NATIVE == inMemoryFormat) {
            key = toData(key);
        }

        return !writeBehindQueue.contains(DelayedEntries.createDefault(key, null, -1, -1));
    }

    @Override
    public int notFinishedOperationsCount() {
        return writeBehindQueue.size();
    }

    @Override
    public Object flush(Data key, Object value, boolean backup) {
        if (NATIVE == inMemoryFormat) {
            key = toData(key);
            value = toData(value);
        }

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

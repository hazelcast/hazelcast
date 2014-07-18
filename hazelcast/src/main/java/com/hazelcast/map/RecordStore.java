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

package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.mapstore.MapDataStore;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines a record-store.
 */
public interface RecordStore {

    String getName();

    Object put(Data dataKey, Object dataValue, long ttl);

    void put(Map.Entry<Data, Object> entry);

    Object putIfAbsent(Data dataKey, Object value, long ttl);

    Record putBackup(Data key, Object value);

    Record putBackup(Data key, Object value, long ttl);

    boolean tryPut(Data dataKey, Object value, long ttl);

    boolean set(Data dataKey, Object value, long ttl);

    Object remove(Data dataKey);

    boolean remove(Data dataKey, Object testValue);

    /**
     * Similar to {@link com.hazelcast.map.RecordStore#remove(com.hazelcast.nio.serialization.Data)}
     * except removeBackup doesn't touch mapstore since it does not return previous value.
     */
    void removeBackup(Data dataKey);

    Object get(Data dataKey);

    MapEntrySet getAll(Set<Data> keySet);

    boolean containsKey(Data dataKey);

    Object replace(Data dataKey, Object value);

    boolean replace(Data dataKey, Object oldValue, Object newValue);

    void putTransient(Data dataKey, Object value, long ttl);

    /**
     * Puts key-value pair to map which is the result of a load from map store operation.
     *
     * @param key   key to put.
     * @param value to put.
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see {@link com.hazelcast.map.operation.PutFromLoadAllOperation}
     */
    Object putFromLoad(Data key, Object value);

    /**
     * Puts key-value pair to map which is the result of a load from map store operation.
     *
     * @param key   key to put.
     * @param value to put.
     * @param ttl   time to live seconds.
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see {@link com.hazelcast.map.operation.PutAllOperation}
     */
    Object putFromLoad(Data key, Object value, long ttl);

    boolean merge(Data dataKey, EntryView mergingEntryView, MapMergePolicy mergePolicy);

    Record getRecord(Data key);

    /**
     * Puts a data key and a record value to record-store.
     * Used in replication operations.
     *
     * @param key    the data key to put record store.
     * @param record the value for record store.
     * @see {@link com.hazelcast.map.operation.MapReplicationOperation}
     */
    void putRecord(Data key, Record record);

    void deleteRecord(Data key);

    /**
     * Iterates over record store values.
     *
     * @return read only iterator for map values.
     */
    Iterator<Record> iterator();

    /**
     * Iterates over record store values but first waits map store to load.
     * If an operation needs to wait a data source load like query operations
     * {@link com.hazelcast.core.IMap#keySet(com.hazelcast.query.Predicate)},
     * this method can be used to return a read-only iterator.
     *
     * @return read only iterator for map values.
     */
    Iterator<Record> loadAwareIterator();

    /**
     * Returns records map.
     *
     * @see com.hazelcast.map.RecordStoreLoader
     */
    Map<Data, Record> getRecordMap();

    Set<Data> keySet();

    int size();

    boolean txnLock(Data key, String caller, long threadId, long ttl);

    boolean extendLock(Data key, String caller, long threadId, long ttl);

    boolean unlock(Data key, String caller, long threadId);

    boolean isLocked(Data key);

    boolean canAcquireLock(Data key, String caller, long threadId);

    String getLockOwnerInfo(Data key);

    boolean containsValue(Object testValue);

    Object evict(Data key);

    /**
     * Evicts all keys except locked ones.
     *
     * @return number of evicted entries.
     */
    int evictAll();

    /**
     * Evicts all keys except locked ones on backup.
     */
    void evictAllBackup();

    Collection<Data> valuesData();

    MapContainer getMapContainer();

    Set<Map.Entry<Data, Data>> entrySetData();

    Map.Entry<Data, Object> getMapEntry(Data dataKey);

    Map.Entry<Data, Object> getMapEntryForBackup(Data dataKey);

    void flush();

    void clearPartition();

    void reset();

    boolean forceUnlock(Data dataKey);

    long getHeapCost();

    boolean isLoaded();

    void checkIfLoaded();

    void setLoaded(boolean loaded);

    int clear();

    boolean isEmpty();

    /**
     * Do expiration operations.
     *
     * @param percentage of max expirables according to the record store size.
     * @param owner      <code>true</code> if an owner partition, otherwise <code>false</code>.
     */
    void evictExpiredEntries(int percentage, boolean owner);

    /**
     * @return <code>true</code> if record store has at least one candidate entry
     * for expiration else return <code>false</code>.
     */
    boolean isExpirable();

    /**
     * Loads all keys from defined map store.
     *
     * @param keys                  keys to be loaded.
     * @param replaceExistingValues <code>true</code> if need to replace existing values otherwise <code>false</code>
     */
    void loadAllFromStore(List<Data> keys, boolean replaceExistingValues);

    MapDataStore<Data, Object> getMapDataStore();

    int getPartitionId();

}

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.monitor.LocalRecordStoreStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Defines a record-store.
 */
public interface RecordStore<R extends Record> extends LocalRecordStoreStats {

    /**
     * Default TTL value of a record.
     */
    long DEFAULT_TTL = -1L;

    String getName();

    Object put(Data dataKey, Object dataValue, long ttl);

    Object putIfAbsent(Data dataKey, Object value, long ttl);

    R putBackup(Data key, Object value);

    /**
     * @param key          the key to be processed.
     * @param value        the value to be processed.
     * @param ttl          milliseconds. Check out {@link com.hazelcast.map.impl.proxy.MapProxySupport#putInternal}
     * @param putTransient {@code true} if putting transient entry, otherwise {@code false}
     * @return previous record if exists otherwise null.
     */
    R putBackup(Data key, Object value, long ttl, boolean putTransient);

    /**
     * Returns {@code true} if key doesn't exist previously, otherwise returns {@code false}.
     *
     * @see com.hazelcast.core.IMap#set(Object, Object)
     */
    boolean set(Data dataKey, Object value, long ttl);

    Object remove(Data dataKey);

    boolean delete(Data dataKey);

    boolean remove(Data dataKey, Object testValue);

    /**
     * Similar to {@link RecordStore#remove(com.hazelcast.nio.serialization.Data)}
     * except removeBackup doesn't touch mapstore since it does not return previous value.
     */
    void removeBackup(Data dataKey);

    /**
     * Gets record from {@link RecordStore}.
     * Loads missing keys from map store.
     *
     * @param dataKey key.
     * @param backup  <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return value of an entry in {@link RecordStore}
     */
    Object get(Data dataKey, boolean backup);

    /**
     * Called when {@link com.hazelcast.config.MapConfig#isReadBackupData} is <code>true</code> from
     * {@link com.hazelcast.map.impl.proxy.MapProxySupport#getInternal}
     * <p/>
     * Returns corresponding value for key as {@link com.hazelcast.nio.serialization.Data}.
     * This adds an extra serialization step. For the reason of this behaviour please see issue 1292 on github.
     *
     * @param key key to be accessed
     * @return value as {@link com.hazelcast.nio.serialization.Data}
     * independent of {@link com.hazelcast.config.InMemoryFormat}
     */
    Data readBackupData(Data key);

    MapEntries getAll(Set<Data> keySet);

    boolean containsKey(Data dataKey);

    int getLockedEntryCount();

    Object replace(Data dataKey, Object update);


    /**
     * Sets the value to the given updated value
     * if {@link com.hazelcast.map.impl.record.RecordFactory#isEquals} comparison
     * of current value and expected value is {@code true}.
     *
     * @param dataKey key which's value is requested to be replaced.
     * @param expect  the expected value
     * @param update  the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    boolean replace(Data dataKey, Object expect, Object update);

    void putTransient(Data dataKey, Object value, long ttl);

    /**
     * Puts key-value pair to map which is the result of a load from map store operation.
     *
     * @param key   key to put.
     * @param value to put.
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see com.hazelcast.map.impl.operation.PutFromLoadAllOperation
     */
    Object putFromLoad(Data key, Object value);

    /**
     * Puts key-value pair to map which is the result of a load from map store operation on backup.
     *
     * @param key   key to put.
     * @param value to put.
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see com.hazelcast.map.impl.operation.PutFromLoadAllBackupOperation
     */
    Object putFromLoadBackup(Data key, Object value);

    /**
     * Puts key-value pair to map which is the result of a load from map store operation.
     *
     * @param key   key to put.
     * @param value to put.
     * @param ttl   time to live seconds.
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see com.hazelcast.map.impl.operation.PutFromLoadAllOperation
     */
    Object putFromLoad(Data key, Object value, long ttl);

    boolean merge(Data dataKey, EntryView mergingEntryView, MapMergePolicy mergePolicy);

    R getRecord(Data key);

    /**
     * Puts a data key and a record value to record-store.
     * Used in replication operations.
     *
     * @param key    the data key to put record store.
     * @param record the value for record store.
     * @see com.hazelcast.map.impl.operation.MapReplicationOperation
     */
    void putRecord(Data key, R record);

    /**
     * Iterates over record store entries.
     *
     * @return read only iterator for map values.
     */
    Iterator<Record> iterator();

    /**
     * Iterates over record store entries by respecting expiration.
     *
     * @return read only iterator for map values.
     */
    Iterator<Record> iterator(long now, boolean backup);

    /**
     * Fetches specified number of keys from provided tableIndex.
     *
     * @return {@link MapKeysWithCursor} which is a holder for keys and next index to read from.
     */
    MapKeysWithCursor fetchKeys(int tableIndex, int size);

    /**
     * Fetches specified number of entries from provided tableIndex.
     *
     * @return {@link MapEntriesWithCursor} which is a holder for entries and next index to read from.
     */
    MapEntriesWithCursor fetchEntries(int tableIndex, int size);

    /**
     * Iterates over record store entries but first waits map store to load.
     * If an operation needs to wait a data source load like query operations
     * {@link com.hazelcast.core.IMap#keySet(com.hazelcast.query.Predicate)},
     * this method can be used to return a read-only iterator.
     *
     * @param now    current time in millis
     * @param backup <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return read only iterator for map values.
     */
    Iterator<Record> loadAwareIterator(long now, boolean backup);

    Set<Data> keySet();

    int size();

    boolean txnLock(Data key, String caller, long threadId, long referenceId, long ttl, boolean blockReads);

    boolean extendLock(Data key, String caller, long threadId, long ttl);

    boolean unlock(Data key, String caller, long threadId, long referenceId);

    boolean isLocked(Data key);

    boolean isTransactionallyLocked(Data key);

    boolean canAcquireLock(Data key, String caller, long threadId);

    String getLockOwnerInfo(Data key);

    boolean containsValue(Object testValue);

    Object evict(Data key, boolean backup);

    /**
     * Evicts all keys except locked ones.
     *
     * @param backup <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return number of evicted entries.
     */
    int evictAll(boolean backup);

    MapContainer getMapContainer();

    /**
     * @see MapDataStore#softFlush()
     */
    long softFlush();

    /**
     * Clears internal partition data.
     *
     * @param onShutdown true if {@code close} is called during MapService shutdown,
     *                   false otherwise.
     */
    void clearPartition(boolean onShutdown);

    /**
     * Resets the record store to it's initial state.
     */
    void reset();

    boolean forceUnlock(Data dataKey);

    long getHeapCost();

    boolean isLoaded();

    void checkIfLoaded() throws RetryableHazelcastException;

    int clear();

    boolean isEmpty();

    /**
     * Do expiration operations.
     *
     * @param percentage of max expirables according to the record store size.
     * @param backup     <code>true</code> if a backup partition, otherwise <code>false</code>.
     */
    void evictExpiredEntries(int percentage, boolean backup);

    /**
     * @return <code>true</code> if record store has at least one candidate entry
     * for expiration else return <code>false</code>.
     */
    boolean isExpirable();

    /**
     * Checks whether a record is expired or not.
     *
     * @param record the record from record-store.
     * @param now    current time in millis
     * @param backup <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return <code>true</code> if the record is expired, <code>false</code> otherwise.
     */
    boolean isExpired(R record, long now, boolean backup);

    /**
     * Does post eviction operations like sending events
     *
     * @param record record to process
     * @param backup <code>true</code> if a backup partition, otherwise <code>false</code>.
     */
    void doPostEvictionOperations(Record record, boolean backup);

    /**
     * Loads all given keys from defined map store.
     *
     * @param keys keys to be loaded.
     */
    void loadAllFromStore(List<Data> keys, boolean withUserSuppliedKeys);

    void updateLoadStatus(boolean lastBatch, Throwable exception);

    MapDataStore<Data, Object> getMapDataStore();

    int getPartitionId();

    /**
     * Returns live record or null if record is already expired. Does not load missing keys from a map store.
     *
     * @param key key to be accessed
     * @return live record or null
     * @see #get
     */
    R getRecordOrNull(Data key);

    /**
     * Evicts entries from this record-store.
     *
     * @param excludedKey this key has lowest priority to be selected for eviction
     */
    void evictEntries(Data excludedKey);

    /**
     * Returns <code>true</code> if eviction is allowed on this record-store, otherwise <code>false</code>
     *
     * @return <code>true</code> if eviction is allowed on this record-store, otherwise <code>false</code>
     */
    boolean shouldEvict();

    /**
     * Loads all keys and values
     *
     * @param replaceExistingValues <code>true</code> if need to replace existing values otherwise <code>false</code>
     **/
    void loadAll(boolean replaceExistingValues);

    /**
     * Performs initial loading from a MapLoader if it has not been done before
     **/
    void maybeDoInitialLoad();

    Storage createStorage(RecordFactory<R> recordFactory, InMemoryFormat memoryFormat);

    Record createRecord(Object value, long ttlMillis, long now);

    Record loadRecordOrNull(Data key, boolean backup);

    /**
     * This can be used to release unused resources.
     */
    void disposeDeferredBlocks();

    void destroy();

    Storage getStorage();

    /**
     * Starts mapLoader
     */
    void startLoading();

    /**
     * Informs this recordStore about the loading status of the recordStore that this store is migrated from.
     * If the 'predecessor' has been loaded this record store should trigger the load again.
     * Will be taken into account only if invoked before the startLoading method. Otherwise has no effect.
     * <p>
     * This method should be deleted when the map's lifecycle has been cleaned-up. Currently it's impossible to
     * pass additional state when the record store is created, thus this this state has to be passed in post-creation
     * setters which is cumbersome and error-prone.
     */
    void setPreMigrationLoadedStatus(boolean loaded);

    /**
     * Initialize the recordStore after creation
     */
    void init();

    /**
     * Register a callback for when key loading is complete
     **/
    void onKeyLoad(ExecutionCallback<Boolean> callback);

}

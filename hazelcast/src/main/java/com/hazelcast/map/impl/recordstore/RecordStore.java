/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.monitor.LocalRecordStoreStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Defines a record-store.
 */
public interface RecordStore<R extends Record> {

    /**
     * Default TTL value of a record.
     */
    long DEFAULT_TTL = -1L;

    /**
     * Default Max Idle value of a record.
     */
    long DEFAULT_MAX_IDLE = -1L;

    LocalRecordStoreStats getLocalRecordStoreStats();

    String getName();

    /**
     * @return oldValue only if it exists in memory, otherwise just returns
     * null and doesn't try to load it from {@link com.hazelcast.core.MapLoader}
     */
    Object set(Data dataKey, Object value, long ttl, long maxIdle);

    /**
     * @return oldValue if it exists in memory otherwise tries to load oldValue
     * by using {@link com.hazelcast.core.MapLoader}
     */
    Object put(Data dataKey, Object dataValue, long ttl, long maxIdle);

    Object putIfAbsent(Data dataKey, Object value, long ttl, long maxIdle, Address callerAddress);

    /**
     * @param key        the key
     * @param value      the value to put backup
     * @param provenance origin of call to this method.
     * @return current record object associated to the key
     */
    R putBackup(Data key, Object value, CallerProvenance provenance);

    /**
     * @param key          the key to be processed.
     * @param value        the value to be processed.
     * @param ttl          milliseconds. Check out {@link com.hazelcast.map.impl.proxy.MapProxySupport#putInternal}
     * @param maxIdle      milliseconds. Check out {@link com.hazelcast.map.impl.proxy.MapProxySupport#putInternal}
     * @param putTransient {@code true} if putting transient entry, otherwise {@code false}
     * @param provenance   origin of call to this method.
     * @return previous record if exists otherwise null.
     */
    R putBackup(Data key, Object value, long ttl, long maxIdle, boolean putTransient, CallerProvenance provenance);

    /**
     * Does exactly the same thing as {@link #set(Data, Object, long, long)} except the invocation is not counted as
     * a read access while updating the access statics.
     */
    boolean setWithUncountedAccess(Data dataKey, Object value, long ttl, long maxIdle);

    /**
     * @param key        the key to be removed
     * @param provenance origin of call to this method.
     * @return value of removed entry or null if there is no matching entry
     */
    Object remove(Data key, CallerProvenance provenance);

    /**
     * @param dataKey    the key to be removed
     * @param provenance origin of call to this method.
     * @return {@code true} if entry is deleted, otherwise returns {@code false}
     */
    boolean delete(Data dataKey, CallerProvenance provenance);

    boolean remove(Data dataKey, Object testValue);

    void setTTL(Data key, long ttl);

    /**
     * Similar to {@link RecordStore##remove(Data, CallerProvenance)}
     * except removeBackup doesn't touch mapstore since it does not return previous value.
     */
    void removeBackup(Data dataKey, CallerProvenance provenance);

    /**
     * Gets record from {@link RecordStore}.
     * Loads missing keys from map store.
     *
     * @param dataKey key.
     * @param backup  <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return value of an entry in {@link RecordStore}
     */
    Object get(Data dataKey, boolean backup, Address callerAddress);

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

    MapEntries getAll(Set<Data> keySet, Address callerAddress);

    /**
     * Checks if the key exist in memory without trying to load data from map-loader
     */
    boolean existInMemory(Data key);

    boolean containsKey(Data dataKey, Address callerAddress);

    int getLockedEntryCount();

    Object replace(Data dataKey, Object update);

    /**
     * Sets the value to the given updated value
     * if {@link com.hazelcast.map.impl.record.RecordComparator#isEqual} comparison
     * of current value and expected value is {@code true}.
     *
     * @param dataKey key which's value is requested to be replaced.
     * @param expect  the expected value
     * @param update  the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    boolean replace(Data dataKey, Object expect, Object update);

    Object putTransient(Data dataKey, Object value, long ttl, long maxIdle);

    /**
     * Puts key-value pair to map which is the result of a load from map store operation.
     *
     * @param key   key to put.
     * @param value to put.
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @see com.hazelcast.map.impl.operation.PutFromLoadAllOperation
     */
    Object putFromLoad(Data key, Object value, Address callerAddress);

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

    boolean merge(MapMergeTypes mergingEntry,
                  SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy);

    /**
     * Merges the given {@link MapMergeTypes} via the given {@link SplitBrainMergePolicy}.
     *
     * @param mergingEntry the {@link MapMergeTypes} instance to merge
     * @param mergePolicy  the {@link SplitBrainMergePolicy} instance to apply
     * @param provenance   origin of call to this method.
     * @return {@code true} if merge is applied, otherwise {@code false}
     */
    boolean merge(MapMergeTypes mergingEntry,
                  SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy,
                  CallerProvenance provenance);

    boolean merge(Data dataKey, EntryView mergingEntry, MapMergePolicy mergePolicy);

    /**
     * Merges the given {@link EntryView} via the given {@link MapMergePolicy}.
     *
     * @param dataKey      the key to be merged
     * @param mergingEntry the {@link EntryView} instance to merge
     * @param mergePolicy  the {@link MapMergePolicy} instance to apply
     * @param provenance   origin of call to this method.
     * @return {@code true} if merge is applied, otherwise {@code false}
     */
    boolean merge(Data dataKey, EntryView mergingEntry, MapMergePolicy mergePolicy,
                  CallerProvenance provenance);

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

    int size();

    boolean txnLock(Data key, String caller, long threadId, long referenceId, long ttl, boolean blockReads);

    boolean extendLock(Data key, String caller, long threadId, long ttl);

    boolean localLock(Data key, String caller, long threadId, long referenceId, long ttl);

    boolean lock(Data key, String caller, long threadId, long referenceId, long ttl);

    boolean isLockedBy(Data key, String caller, long threadId);

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

    boolean forceUnlock(Data dataKey);

    long getOwnedEntryCost();

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

    MapDataStore<Data, Object> getMapDataStore();

    InvalidationQueue<ExpiredKey> getExpiredKeys();

    /**
     * Returns the partition id this RecordStore belongs to.
     *
     * @return the partition id.
     */
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

    Storage createStorage(RecordFactory<R> recordFactory, InMemoryFormat memoryFormat);

    Record createRecord(Object value, long ttlMillis, long maxIdle, long now);

    Record loadRecordOrNull(Data key, boolean backup, Address callerAddress);

    /**
     * This can be used to release unused resources.
     */
    void disposeDeferredBlocks();

    /**
     * Initialize the recordStore after creation
     */
    void init();

    Storage getStorage();

    /**
     * Starts the map loader if there is a configured and enabled
     * {@link com.hazelcast.core.MapLoader} and the key loading has not already
     * been started.
     * The loading may start again if there was a migration and the record store
     * on the migration source has started but not completed the loading.
     */
    void startLoading();

    /**
     * Informs this recordStore about the loading status of the recordStore
     * that this store is migrated from.
     * If the record store on the migration source has been loaded then this
     * record store should NOT trigger the load again.
     * Will be taken into account only if invoked before {@link #startLoading()},
     * otherwise has no effect.
     * <p>
     * This method should be deleted when the map's lifecycle has been
     * cleaned-up. Currently it's impossible to pass additional state when
     * the record store is created, thus this state has to be passed in
     * post-creation setters which is cumbersome and error-prone.
     */
    void setPreMigrationLoadedStatus(boolean loaded);

    /**
     * @return {@code true} if the key loading and dispatching has finished on
     * this record store
     */
    boolean isKeyLoadFinished();

    /**
     * Returns {@code true} if all key and value loading tasks have completed
     * on this record store.
     */
    boolean isLoaded();

    void checkIfLoaded()
            throws RetryableHazelcastException;

    /**
     * Triggers key and value loading if there is no ongoing or completed
     * key loading task, otherwise does nothing.
     * The actual loading is done on a separate thread.
     *
     * @param replaceExistingValues if the existing entries for the loaded keys should be replaced
     */
    void loadAll(boolean replaceExistingValues);

    /**
     * Resets the map loader state if necessary and triggers initial key and
     * value loading if it has not been done before.
     */
    void maybeDoInitialLoad();

    /**
     * Triggers loading values for the given {@code keys} from the
     * defined {@link com.hazelcast.core.MapLoader}.
     * The values will be loaded asynchronously and this method will
     * return as soon as the value loading task has been offloaded
     * to a different thread.
     *
     * @param keys                  the keys for which values will be loaded
     * @param replaceExistingValues if the existing entries for the keys should
     *                              be replaced with the loaded values
     */
    void loadAllFromStore(List<Data> keys, boolean replaceExistingValues);

    /**
     * Advances the state of the map key loader for this partition and sets the key
     * loading future result if the {@code lastBatch} is {@code true}.
     * <p>
     * If there was an exception during key loading, you may pass it as the
     * {@code exception} paramter and it will be set as the result of the future.
     *
     * @param lastBatch if the last key batch was sent
     * @param exception an exception that occurred during key loading
     */
    void updateLoadStatus(boolean lastBatch, Throwable exception);

    /**
     * @return true if there is a {@link com.hazelcast.map.QueryCache} defined
     * for this map.
     */
    boolean hasQueryCache();

    /**
     * Called by {@link IMap#destroy()} or {@link
     * com.hazelcast.map.impl.MapMigrationAwareService}
     *
     * Clears internal partition data.
     *
     * @param onShutdown           true if {@code close} is called during
     *                             MapService shutdown, false otherwise.
     * @param onRecordStoreDestroy true if record-store will be destroyed,
     *                             otherwise false.
     */
    void clearPartition(boolean onShutdown, boolean onRecordStoreDestroy);

    /**
     * Called by {@link IMap#clear()}.
     *
     * Clears data in this record store.
     *
     * @return number of cleared entries.
     */
    int clear();

    /**
     * Resets the record store to it's initial state.
     *
     * Used in replication operations.
     *
     * @see #putRecord(Data, Record)
     */
    void reset();

    /**
     * Called by {@link IMap#destroy()}.
     *
     * Destroys data in this record store.
     */
    void destroy();

    /**
     * Like {@link #destroy()} but does not touch state on other services
     * like lock service or event journal service.
     */
    void destroyInternals();
}

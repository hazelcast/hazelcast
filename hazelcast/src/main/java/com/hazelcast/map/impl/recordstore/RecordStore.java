/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.internal.monitor.LocalRecordStoreStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Defines a record-store.
 */
public interface RecordStore<R extends Record> {

    LocalRecordStoreStats getLocalRecordStoreStats();

    String getName();

    /**
     * @return old value
     */
    Object set(Data dataKey, Object value, long ttl, long maxIdle);

    /**
     * @return old value
     */
    Object setTxn(Data dataKey, Object value, long ttl, long maxIdle, UUID transactionId);

    Object removeTxn(Data dataKey, CallerProvenance callerProvenance, UUID transactionId);

    /**
     * @return old value
     */
    Object put(Data dataKey, Object dataValue, long ttl, long maxIdle);

    Object putIfAbsent(Data dataKey, Object value, long ttl, long maxIdle, Address callerAddress);

    /**
     * @param key        the key
     * @param value      the value to put backup
     * @param provenance origin of call to this method.
     * @return current record after put.
     */
    R putBackup(Data key, Object value, CallerProvenance provenance);

    /**
     * @return current record after put.
     */
    R putBackup(Record record, boolean putTransient, CallerProvenance provenance);

    /**
     * @return current record after put.
     */
    R putBackupTxn(Record newRecord, boolean putTransient,
                   CallerProvenance provenance, UUID transactionId);

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

    boolean setTtl(Data key, long ttl, boolean backup);

    /**
     * Checks whether ttl or maxIdle are set on the record.
     *
     * @param record the record to be checked
     * @return {@code true} if ttl or maxIdle are defined on the {@code record}, otherwise {@code false}.
     */
    boolean isTtlOrMaxIdleDefined(Record record);

    /**
     * Callback which is called when the record is being accessed from the record or index store.
     * <p>
     * An implementation is not supposed to be thread safe.
     *
     * @param record the accessed record
     * @param now    the current time
     */
    void accessRecord(Record record, long now);

    /**
     * Similar to {@link RecordStore#remove(Data, CallerProvenance)}
     * except removeBackup doesn't touch mapstore since it does not return previous value.
     */
    void removeBackup(Data dataKey, CallerProvenance provenance);

    void removeBackupTxn(Data dataKey, CallerProvenance callerProvenance, UUID transactionId);

    /**
     * Gets record from {@link RecordStore}.
     * Loads missing keys from map store.
     *
     * @param dataKey key.
     * @param backup  {@code true} if a backup partition, otherwise {@code false}.
     * @param touch   when {@code true}, if an existing record was found for the given key,
     *                then its last access time is updated.
     * @return value of an entry in {@link RecordStore}
     */
    Object get(Data dataKey, boolean backup, Address callerAddress, boolean touch);

    /**
     * Same as {@link #get(Data, boolean, Address, boolean)} with parameter {@code touch}
     * set {@code true}.
     */
    default Object get(Data dataKey, boolean backup, Address callerAddress) {
        return get(dataKey, backup, callerAddress, true);
    }

    /**
     * Called when {@link com.hazelcast.config.MapConfig#isReadBackupData} is <code>true</code> from
     * {@link com.hazelcast.map.impl.proxy.MapProxySupport#getInternal}
     * <p>
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
     * if {@link ValueComparator#isEqual} comparison
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

    Object putFromLoad(Data key, Object value, long expirationTime, Address callerAddress);

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

    Object putFromLoadBackup(Data key, Object value, long expirationTime);

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

    R getRecord(Data key);

    /**
     * Puts a data key and a record value to record-store.
     * Used in replication operations.
     *
     * @param record      the value for record store.
     * @param nowInMillis nowInMillis
     * @param indexesMustBePopulated
     * @return current record after put
     * @see com.hazelcast.map.impl.operation.MapReplicationOperation
     */
    Record putReplicatedRecord(R record, long nowInMillis, boolean indexesMustBePopulated);

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
     * {@link IMap#keySet(com.hazelcast.query.Predicate)},
     * this method can be used to return a read-only iterator.
     *
     * @param now    current time in millis
     * @param backup <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return read only iterator for map values.
     */
    Iterator<Record> loadAwareIterator(long now, boolean backup);

    int size();

    boolean txnLock(Data key, UUID caller, long threadId, long referenceId, long ttl, boolean blockReads);

    boolean extendLock(Data key, UUID caller, long threadId, long ttl);

    boolean localLock(Data key, UUID caller, long threadId, long referenceId, long ttl);

    boolean lock(Data key, UUID caller, long threadId, long referenceId, long ttl);

    boolean isLockedBy(Data key, UUID caller, long threadId);

    boolean unlock(Data key, UUID caller, long threadId, long referenceId);

    boolean isLocked(Data key);

    boolean isTransactionallyLocked(Data key);

    boolean canAcquireLock(Data key, UUID caller, long threadId);

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
     */
    void doPostEvictionOperations(Record record);

    MapDataStore<Data, Object> getMapDataStore();

    InvalidationQueue<ExpiredKey> getExpiredKeysQueue();

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
     * Check if record is reachable according to TTL or idle times.
     * If not reachable return null.
     *
     * @param record the record from record-store.
     * @param now    current time in millis
     * @param backup <code>true</code> if a backup partition, otherwise <code>false</code>.
     * @return null if evictable.
     */
    R getOrNullIfExpired(R record, long now, boolean backup);


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

    Record createRecord(Data key, Object value, long ttlMillis, long maxIdle, long now);

    /**
     * Creates a new record from a replicated record
     * by making memory format related conversions.
     */
    Record createRecord(Record fromRecord, long nowInMillis);

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

    void sampleAndForceRemoveEntries(int entryCountToRemove);

    /**
     * Starts the map loader if there is a configured and enabled
     * {@link MapLoader} and the key loading has not already
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

    void checkIfLoaded() throws RetryableHazelcastException;

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
     * defined {@link MapLoader}.
     * The values will be loaded asynchronously and this method will
     * return as soon as the value loading task has been offloaded
     * to a different thread.
     *
     * @param keys                  the keys for which values will be loaded
     * @param replaceExistingValues if the existing entries for the keys should
     *                              be replaced with the loaded values
     */
    void loadAllFromStore(List<Data> keys,
                          boolean replaceExistingValues);

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
    void clearPartition(boolean onShutdown,
                        boolean onRecordStoreDestroy);

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

    InMemoryFormat getInMemoryFormat();
}

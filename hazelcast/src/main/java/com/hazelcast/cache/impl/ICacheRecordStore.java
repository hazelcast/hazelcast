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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * {@link ICacheRecordStore} is the core contract providing internal functionality to
 * {@link com.hazelcast.cache.ICache} implementations on partition scope. All of the ICache methods actually
 * map to a method on this interface through Hazelcast's RPC mechanism. Hazelcast
 * {@link Operation} is sent to the relevant partition to be executed and the final
 * results are returned to the callers.
 * <p>
 * For each partition, there is only one {@link ICacheRecordStore} in the cluster.
 * <p>Implementations of this interface may provide different internal data persistence like on-heap storage.</p>
 * Each expirible cache entry is actually a {@link Data}, {@link CacheRecord} pair.
 * <p>Key type: always serialized form of {@link Data}.</p>
 * <p>Value types: depend on the configuration.</p>
 *
 * @see com.hazelcast.cache.impl.CacheRecordStore
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ICacheRecordStore {

    /**
     * Gets the value to which the specified key is mapped,
     * or {@code null} if this cache contains no mapping for the key.
     * <p>
     * If the cache is configured to use read-through, and get would return null
     * because the entry is missing from the cache, the Cache's {@link javax.cache.integration.CacheLoader}
     * is called in an attempt to load the entry.
     * </p>
     *
     * @param key          the key whose associated value is to be returned.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @return the element, or null, if it does not exist.
     */
    Object get(Data key, ExpiryPolicy expiryPolicy);

    /**
     * Associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A cache
     * <tt>c</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #contains(Data) c.contains(k)} would return
     * <tt>true</tt>.)
     * <p>
     * The previous value is returned, or null if there was no value associated
     * with the key previously.
     *
     * @param key          key with which the specified value is to be associated.
     * @param value        value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @param caller       UUID of the calling node or client.
     * @return the stored {@link CacheRecord}  (added as new record or updated). <code>null</code> if record has expired.
     */
    CacheRecord put(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller, int completionId);

    /**
     * Associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A cache
     * <tt>c</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #contains(Data) c.contains(k)} would return
     * <tt>true</tt>.)
     * <p>
     * The previous value is returned, or null if there was no value associated
     * with the key previously.
     *
     * @param key          key with which the specified value is to be associated.
     * @param value        value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @param caller       UUID of the calling node or client.
     * @return the value associated with the key at the start of the operation or
     * null if none was associated.
     */
    Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller, int completionId);

    /**
     * Removes the mapping for a key from this cache if it is present.
     * <p>
     * More formally, if this cache contains a mapping from key <tt>k</tt> to
     * value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.
     * (The cache can contain at most one such mapping.)
     * <p>
     * <p>Returns <tt>true</tt> if this cache previously associated the key,
     * or <tt>false</tt> if the cache contained no mapping for the key.
     * <p>
     * The cache will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key          key with which the specified value is to be associated.
     * @param value        value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @param caller       UUID of the calling node or client.
     * @return true if a value was set..
     */
    boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller, int completionId);

    /**
     * Atomically removes the entry for a key only if it is currently mapped to some
     * value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.remove(key);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key    key with which the specified value is associated.
     * @param caller UUID of the calling node or client.
     * @return the value if one existed or null if no mapping existed for this key.
     */
    Object getAndRemove(Data key, UUID caller, int completionId);

    /**
     * Removes the mapping for a key from this cache if it is present.
     * <p>
     * More formally, if this cache contains a mapping from key <tt>k</tt> to
     * value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.
     * (The cache can contain at most one such mapping.)
     * <p>
     * <p>Returns <tt>true</tt> if this cache previously associated the key,
     * or <tt>false</tt> if the cache contained no mapping for the key.
     * <p>
     * The cache will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key          key whose mapping is to be removed from the cache.
     * @param caller       UUID of the calling node or client.
     * @param origin       Source of the call
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @param provenance   caller operation provenance
     * @return returns false if there was no matching key.
     */
    boolean remove(Data key, UUID caller, UUID origin, int completionId, CallerProvenance provenance);

    boolean remove(Data key, UUID caller, UUID origin, int completionId);


    /**
     * Atomically removes the mapping for a key only if currently mapped to the
     * given value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *   cache.remove(key);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key          key whose mapping is to be removed from the cache.
     * @param value        value expected to be associated with the specified key.
     * @param caller       UUID of the calling node or client.
     * @param origin       Source of the call
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return returns false if there was no matching key.
     */
    boolean remove(Data key, Object value, UUID caller, UUID origin, int completionId);

    /**
     * Atomically replaces the entry for a key only if currently mapped to some
     * value.
     * <p>
     * This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key          the key with which the specified value is associated.
     * @param value        the value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @param caller       UUID of the calling node or client.
     * @return <tt>true</tt> if the value was replaced.
     */
    boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller, int completionId);

    /**
     * Atomically replaces the entry for a key only if currently mapped to a
     * given value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue)) {
     *  cache.put(key, newValue);
     * return true;
     * } else {
     *  return false;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key          key with which the specified value is associated.
     * @param oldValue     value expected to be associated with the specified key.
     * @param newValue     value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @param caller       UUID of the calling node or client.
     * @return <tt>true</tt> if the value was replaced.
     */
    boolean replace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy, UUID caller, int completionId);

    /**
     * Atomically replaces the value for a given key if and only if there is a
     * value currently mapped by the key.
     * <p>
     * This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.put(key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key          key with which the specified value is associated.
     * @param value        value to be associated with the specified key.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @param caller       uuid of the calling node or client.
     * @return the previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key.
     */
    Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller, int completionId);


    /**
     * Sets expiry policy for the records with given keys if and only if there is a
     * value currently mapped by the key
     *
     * @param keys         keys for the entries
     * @param expiryPolicy custom expiry policy or null to use configured default value
     */
    boolean setExpiryPolicy(Collection<Data> keys, Object expiryPolicy, UUID source);

    Object getExpiryPolicy(Data key);

    /**
     * Determines if this store contains an entry for the specified key.
     * <p>
     * More formally, returns <tt>true</tt> if and only if this store contains a
     * mapping for a key <tt>k</tt> such that <tt>key.equals(k)</tt>
     * (There can be at most one such mapping.)
     *
     * @param key key whose presence in this store is to be tested.
     * @return <tt>true</tt> if this map contains a mapping for the specified key.
     */
    boolean contains(Data key);

    /**
     * Gets a collection of entries from the store, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     * <p>
     * If the cache is configured read-through, and a get for a key would
     * return null because an entry is missing from the cache, the Cache's
     * {@link javax.cache.integration.CacheLoader} is called in an attempt to load the entry. If an
     * entry cannot be loaded for a given key, the key will not be present in
     * the returned Map.
     *
     * @param keySet       keys whose associated values are to be returned.
     * @param expiryPolicy custom expiry policy or null to use configured default value.
     * @return A simple wrapper for map of entries that were found for the given keys. Keys not found
     * in the cache are not in the result.
     */
    MapEntries getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy);

    /**
     * Calculates the entry size of this store which reflects the partition size of the cache.
     *
     * @return partition size of the cache.
     */
    int size();

    /**
     * clears all internal data without publishing any events
     */
    void clear();

    /**
     * Resets the record store to it's initial state.
     * Used in replication operations.
     */
    void reset();

    /**
     * records of keys will be deleted one by one and will publish a REMOVE event
     * for each key.
     *
     * @param keys set of keys to be cleaned.
     */
    void removeAll(Set<Data> keys, int completionId);

    /**
     * Initializes record store.
     */
    void init();

    /**
     * Close is equivalent to below operations in the given order:
     * <ul>
     * <li>close resources.</li>
     * <li>unregister all listeners.</li>
     * </ul>
     *
     * @param onShutdown true if {@code close} is called during CacheService shutdown,
     *                   false otherwise.
     * @see #clear()
     * @see #destroy()
     */
    void close(boolean onShutdown);

    /**
     * Destroy is equivalent to below operations in the given order:
     * <ul>
     * <li>clear all.</li>
     * <li>close resources.</li>
     * <li>unregister all listeners.</li>
     * </ul>
     *
     * @see #clear()
     * @see #close(boolean)
     */
    void destroy();

    /**
     * Like {@link #destroy()} but does not touch state on other services
     * like event journal service.
     */
    void destroyInternals();

    /**
     * Gets the configuration of the cache that this store belongs to.
     *
     * @return {@link CacheConfig}
     */
    CacheConfig getConfig();

    /**
     * Gets the name of the distributed object name of the cache.
     *
     * @return name.
     */
    String getName();

    /**
     * Returns a readonly map of the internal key value store.
     *
     * @return readonly map of the internal key value store.
     */
    Map<Data, CacheRecord> getReadOnlyRecords();

    boolean isExpirable();

    /**
     * Gets internal record of the store by key.
     *
     * @param key the key to the entry.
     * @return {@link CacheRecord} instance mapped.
     */
    CacheRecord getRecord(Data key);

    /**
     * Associates the specified record with the specified key.
     * This is simply a put operation on the internal map data
     * without any CacheLoad. It also <b>DOES</b> trigger eviction!
     *
     * @param key           the key to the entry.
     * @param record        the value to be associated with the specified key.
     * @param updateJournal when true an event is appended to related event-journal
     */
    void putRecord(Data key, CacheRecord record, boolean updateJournal);

    /**
     * Removes the record for a key.
     *
     * @param key the key to the entry.
     * @return the removed record if one exists.
     */
    CacheRecord removeRecord(Data key);

    /**
     * Fetch minimally {@code size} keys from the {@code pointers} position.
     * The key is fetched on-heap.
     * The method may return less keys if iteration has completed.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items.
     * This can happen if we cannot easily resume from the last returned item
     * by receiving the {@code tableIndex} of the last item. The index can
     * represent a bucket with multiple items and in this case the returned
     * object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param pointers the pointers defining the state of iteration
     * @param size     the minimal count of returned items, unless iteration has completed
     * @return fetched keys and the new iteration state
     */
    CacheKeysWithCursor fetchKeys(IterationPointer[] pointers, int size);

    /**
     * Fetch minimally {@code size} items from the {@code pointers} position.
     * Both the key and value are fetched on-heap.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items.
     * This can happen if we cannot easily resume from the last returned item
     * by receiving the {@code tableIndex} of the last item. The index can
     * represent a bucket with multiple items and in this case the returned
     * object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param pointers the pointers defining the state of iteration
     * @param size     the minimal count of returned items
     * @return fetched entries and the new iteration state
     */
    CacheEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size);

    /**
     * Invokes an {@link EntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     * <p>
     *
     * @param key            the key of the entry.
     * @param entryProcessor the {@link EntryProcessor} to be invoked.
     * @param arguments      additional arguments to be passed to the
     *                       {@link EntryProcessor}.
     * @return the result of the processing, if any, defined by the
     * {@link EntryProcessor} implementation.
     */
    Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments, int completionId);

    /**
     * Synchronously loads the specified entries into the cache using the
     * configured {@link javax.cache.integration.CacheLoader} for the given keys.
     * <p>
     * If an entry for a key already exists in the cache, a value will be loaded
     * if and only if <code>replaceExistingValues</code> is true.  If no loader
     * is configured for the cache, no objects will be loaded.
     *
     * @param keys                  the keys to be loaded.
     * @param replaceExistingValues when true, existing values in the cache will
     *                              be replaced by those loaded from a CacheLoader.
     * @return Set of keys which are successfully loaded.
     */
    Set<Data> loadAll(Set<Data> keys, boolean replaceExistingValues);

    /**
     * Gets the Cache statistics associated with this {@link com.hazelcast.cache.impl.CacheService}.
     *
     * @return {@link CacheStatisticsImpl} cache statistics.
     */
    CacheStatisticsImpl getCacheStats();

    /**
     * Evict cache record store if eviction is required.
     * <p>Eviction logic is handled as specified {@link com.hazelcast.config.EvictionPolicy}
     * in {@link com.hazelcast.config.CacheConfig} for this record store</p>
     *
     * @return true is an entry was evicted, otherwise false
     */
    boolean evictIfRequired();

    void sampleAndForceRemoveEntries(int count);

    /**
     * Determines whether wan replication is enabled or not for this record store.
     *
     * @return <tt>true</tt> if wan replication is enabled for this record store, <tt>false</tt> otherwise
     */
    boolean isWanReplicationEnabled();

    /**
     * Returns {@link ObjectNamespace} associated with this record store.
     *
     * @return ObjectNamespace associated with this record store.
     */
    ObjectNamespace getObjectNamespace();

    /**
     * Merges the given {@link CacheMergeTypes} via the given {@link SplitBrainMergePolicy}.
     *
     * @param mergingEntry     the {@link CacheMergeTypes} instance to merge
     * @param mergePolicy      the {@link SplitBrainMergePolicy} instance to apply
     * @param callerProvenance
     * @return the used {@link CacheRecord} if merge is applied, otherwise {@code null}
     */
    CacheRecord merge(CacheMergeTypes<Object, Object> mergingEntry,
                      SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy,
                      CallerProvenance callerProvenance);

    /**
     * @return partition ID of this store
     */
    int getPartitionId();

    /**
     * Do expiration operations.
     *
     * @param percentage of max expirables according to the record store size.
     */
    void evictExpiredEntries(int percentage);

    InvalidationQueue<ExpiredKey> getExpiredKeysQueue();

    void disposeDeferredBlocks();

    SerializationService getSerializationService();
}

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

package com.hazelcast.core;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMultiMapStats;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A specialized map whose keys can be associated with multiple values.
 * <p/>
 * <p>
 * <b>Gotchas:</b>
 * <ul>
 * <li>
 * Methods -- including but not limited to <tt>get</tt>, <tt>containsKey</tt>,
 * <tt>containsValue</tt>, <tt>remove</tt>, <tt>put</tt>,
 * <tt>lock</tt>, and <tt>unlock</tt>  -- do not use <tt>hashCode</tt> and <tt>equals</tt>
 * implementations of the keys.
 * Instead, they use <tt>hashCode</tt> and <tt>equals</tt> of the binary (serialized) forms of the objects.
 * </li>
 * <li>
 * Methods -- including but not limited to <tt>get</tt>, <tt>remove</tt>,
 * <tt>keySet</tt>, <tt>values</tt>, <tt>entrySet</tt> --
 * return a collection clone of the values. The collection is <b>NOT</b> backed by the map,
 * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
 * </li>
 * </ul>
 * </p>
 *
 * @author oztalip
 * @see IMap
 */
public interface MultiMap<K, V>
        extends BaseMultiMap<K, V>, DistributedObject {

    /**
     * Returns the name of this multimap.
     *
     * @return the name of this multimap
     */
    String getName();

    /**
     * Stores a key-value pair in the multimap.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param key   the key to be stored
     * @param value the value to be stored
     * @return true if size of the multimap is increased, false if the multimap
     * already contains the key-value pair.
     */
    boolean put(K key, V value);

    /**
     * Returns the collection of values associated with the key.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     *
     * @param key the key whose associated values are to be returned
     * @return the collection of the values associated with the key.
     */
    Collection<V> get(K key);

    /**
     * Removes the given key value pair from the multimap.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key   the key of the entry to remove
     * @param value the value of the entry to remove
     * @return true if the size of the multimap changed after the remove operation, false otherwise.
     */
    boolean remove(Object key, Object value);

    /**
     * Removes all the entries with the given key.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     *
     * @param key the key of the entries to remove
     * @return the collection of removed values associated with the given key. The returned collection
     * might be modifiable but it has no effect on the multimap.
     */
    Collection<V> remove(Object key);

    /**
     * Returns the locally owned set of keys.
     * <p/>
     * Each key in this map is owned and managed by a specific
     * member in the cluster.
     * <p/>
     * Note that ownership of these keys might change over time
     * so that key ownerships can be almost evenly distributed
     * in the cluster.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     *
     * @return the locally owned keys
     */
    Set<K> localKeySet();

    /**
     * Returns the set of keys in the multimap.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     *
     * @return the set of keys in the multimap. The returned set might be modifiable
     * but it has no effect on the multimap.
     */
    Set<K> keySet();

    /**
     * Returns the collection of values in the multimap.
     * <p/>
     * <p><b>Warning:</b></p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     *
     * @return the collection of values in the multimap. the returned collection might be modifiable
     * but it has no effect on the multimap.
     */
    Collection<V> values();

    /**
     * Returns the set of key-value pairs in the multimap.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     *
     * @return the set of key-value pairs in the multimap. The returned set might be modifiable
     * but it has no effect on the multimap.
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Returns whether the multimap contains an entry with the key.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param key the key whose existence is checked.
     * @return true if the multimap contains an entry with the key, false otherwise.
     */
    boolean containsKey(K key);

    /**
     * Returns whether the multimap contains an entry with the value.
     * <p/>
     *
     * @param value the value whose existence is checked.
     * @return true if the multimap contains an entry with the value, false otherwise.
     */
    boolean containsValue(Object value);

    /**
     * Returns whether the multimap contains the given key-value pair.
     * <p/>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key   the key whose existence is checked.
     * @param value the value whose existence is checked.
     * @return true if the multimap contains the key-value pair, false otherwise.
     */
    boolean containsEntry(K key, V value);

    /**
     * Returns the number of key-value pairs in the multimap.
     *
     * @return the number of key-value pairs in the multimap.
     */
    int size();

    /**
     * Clears the multimap. Removes all key-value pairs.
     */
    void clear();

    /**
     * Returns the number of values that match the given key in the multimap.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param key the key whose values count is to be returned
     * @return the number of values that match the given key in the multimap
     */
    int valueCount(K key);

    /**
     * Adds a local entry listener for this multimap. The added listener will be only
     * listening for the events (add/remove/update) of the locally owned entries.
     * <p/>
     * Note that entries in distributed multimap are partitioned across
     * the cluster members; each member owns and manages some portion of the
     * entries. Owned entries are called local entries. This
     * listener will be listening for the events of local entries. Let's say
     * your cluster has member1 and member2. On member2 you added a local listener, and from
     * member1, you call <code>multimap.put(key2, value2)</code>.
     * If the key2 is owned by member2, then the local listener will be
     * notified for the add/update event. Also note that entries can migrate to
     * other nodes for load balancing and/or membership change.
     *
     * @param listener entry listener for this multimap
     * @return returns registration id for the entry listener
     * @see #localKeySet()
     */
    String addLocalEntryListener(EntryListener<K, V> listener);

    /**
     * Adds an entry listener for this multimap. The listener will be notified
     * for all multimap add/remove/update/evict events.
     *
     * @param listener     entry listener for this multimap
     * @param includeValue true if <tt>EntryEvent</tt> should
     *                     contain the value, false otherwise
     * @return returns registration id for the entry listener
     */
    String addEntryListener(EntryListener<K, V> listener, boolean includeValue);

    /**
     * Removes the specified entry listener.
     * Returns silently if no such listener was added before.
     *
     * @param registrationId  registration id of listener
     * @return true if registration is removed, false otherwise
     */
    boolean removeEntryListener(String registrationId);

    /**
     * Adds the specified entry listener for the specified key.
     * The listener will be notified for all
     * add/remove/update/evict events for the specified key only.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param listener     the entry listener
     * @param key          the key to listen to
     * @param includeValue true if <tt>EntryEvent</tt> should
     *                     contain the value, false otherwise
     * @return returns registration id.
     */
    String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue);

    /**
     * Acquires a lock for the specified key.
     * <p>If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * The scope of the lock is for this multimap only.
     * The acquired lock is only for the key in this multimap.
     * <p/>
     * Locks are re-entrant, so if the key is locked N times, then
     * it should be unlocked N times before another thread can acquire it.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param key the key to lock.
     */
    void lock(K key);

    /**
     * Acquires the lock for the specified key for the specified lease time.
     * <p>After the lease time, the lock will be released.
     * <p/>
     * <p>If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * Scope of the lock is for this map only.
     * The acquired lock is only for the key in this map.
     * <p/>
     * Locks are re-entrant, so if the key is locked N times, then
     * it should be unlocked N times before another thread can acquire it.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key       the key to lock.
     * @param leaseTime time to wait before releasing the lock.
     * @param timeUnit  unit of time for the lease time.
     */
    void lock(K key, long leaseTime, TimeUnit timeUnit);

    /**
     * Checks the lock for the specified key.
     * <p>If the lock is acquired, this method returns true, else it returns false.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key key to lock to be checked.
     * @return true if the lock is acquired, false otherwise.
     */
    boolean isLocked(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>If the lock is not available, then the current thread
     * does not wait and the method returns false immediately.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param key the key to lock.
     * @return true if lock is acquired, false otherwise.
     */
    boolean tryLock(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>the lock is acquired by the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param time     the maximum time to wait for the lock
     * @param timeunit the time unit of the <tt>time</tt> argument
     * @return true if the lock was acquired, false
     * if the waiting time elapsed before the lock was acquired.
     */
    boolean tryLock(K key, long time, TimeUnit timeunit)
            throws InterruptedException;

    /**
     * Releases the lock for the specified key. It never blocks and
     * returns immediately.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     *
     * @param key the key to lock.
     */
    void unlock(K key);

    /**
     * Releases the lock for the specified key regardless of the lock owner.
     * It always successfully unlocks the key, never blocks
     * and returns immediately.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to lock.
     */
    void forceUnlock(K key);

    /**
     * Returns <tt>LocalMultiMapStats</tt> for this map.
     * <tt>LocalMultiMapStats</tt> is the statistics for the local portion of this
     * distributed multi map and contains information such as ownedEntryCount
     * backupEntryCount, lastUpdateTime, and lockedEntryCount.
     * <p/>
     * Since this stats are only for the local portion of this multi map, if you
     * need the cluster-wide MultiMapStats, then you need to get the {@link com.hazelcast.monitor.LocalMapStats LocalMapStats}
     * from all members of the cluster and combine them.
     *
     * @return this multimap's local statistics.
     */
    LocalMultiMapStats getLocalMultiMapStats();

    /**
     * Executes a predefined aggregation on the multimaps data set. The {@link com.hazelcast.mapreduce.aggregation.Supplier}
     * is used to either select or to select and extract a (sub-)value. A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the multimap
     * @param aggregation     the aggregation that is being executed against the multimap
     * @param <SuppliedValue> the final type emitted from the supplier
     * @param <Result>        the resulting aggregation value type
     * @return Returns the aggregated value for the multimaps data set
     */
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation);

    /**
     * Executes a predefined aggregation on the multimaps data set. The {@link com.hazelcast.mapreduce.aggregation.Supplier}
     * is used to either select, or to select and extract, a (sub-)value. A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the multimap
     * @param aggregation     the aggregation that is being executed against the multimap
     * @param jobTracker      the {@link com.hazelcast.mapreduce.JobTracker} instance to execute the aggregation
     * @param <SuppliedValue> the final type emitted from the supplier
     * @param <Result>        the resulting aggregation value type
     * @return Returns the aggregated value
     */
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation,
                                             JobTracker jobTracker);
}

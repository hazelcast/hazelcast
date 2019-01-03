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
 * <p>
 * <b>Gotchas:</b>
 * <ul>
 * <li>
 * Methods -- including but not limited to {@code get}, {@code containsKey},
 * {@code containsValue}, {@code remove}, {@code put},
 * {@code lock}, and {@code unlock}  -- do not use {@code hashCode} and {@code equals}
 * implementations of the keys.
 * Instead, they use {@code hashCode} and {@code equals} of the binary (serialized) forms of the objects.
 * </li>
 * <li>
 * Methods -- including but not limited to {@code get}, {@code remove},
 * {@code keySet}, {@code values}, {@code entrySet} --
 * return a collection clone of the values. The collection is <b>NOT</b> backed by the map,
 * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
 * </li>
 * </ul>
 * <p>
 * Supports Quorum {@link com.hazelcast.config.QuorumConfig} since 3.10 in cluster versions 3.10 and higher.
 *
 * @param <K> type of the multimap key
 * @param <V> type of the multimap value
 * @see IMap
 */
@SuppressWarnings("checkstyle:methodcount")
public interface MultiMap<K, V> extends BaseMultiMap<K, V> {
    /**
     * Stores a key-value pair in the multimap.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key   the key to be stored
     * @param value the value to be stored
     * @return {@code true} if size of the multimap is increased,
     * {@code false} if the multimap already contains the key-value pair
     */
    boolean put(K key, V value);

    /**
     * Returns the collection of values associated with the key.
     * <p>
     * <b>Warning 1:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b> The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection,
     * and vice-versa.
     *
     * @param key the key whose associated values are to be returned
     * @return the collection of the values associated with the key
     */
    Collection<V> get(K key);

    /**
     * Removes the given key value pair from the multimap.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key   the key of the entry to remove
     * @param value the value of the entry to remove
     * @return {@code true} if the size of the multimap changed after the remove operation,
     * {@code false} otherwise
     */
    boolean remove(Object key, Object value);

    /**
     * Removes all the entries with the given key.
     * <p>
     * <b>Warning 1:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b> The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection,
     * and vice-versa.
     *
     * @param key the key of the entries to remove
     * @return the collection of removed values associated with the given key.
     * The returned collection might be modifiable but it has no effect on the multimap.
     */
    Collection<V> remove(Object key);

    /**
     * Deletes all the entries with the given key.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key the key of the entry to remove
     */

    void delete(Object key);

    /**
     * Returns the locally owned set of keys.
     * <p>
     * Each key in this map is owned and managed by a specific member in the
     * cluster.
     * <p>
     * Note that ownership of these keys might change over time so that key
     * ownerships can be almost evenly distributed in the cluster.
     * <p>
     * <b>Warning:</b> The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection,
     * and vice-versa.
     *
     * @return the locally owned keys
     */
    Set<K> localKeySet();

    /**
     * Returns the set of keys in the multimap.
     * <p>
     * <b>Warning:</b> The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection,
     * and vice-versa.
     *
     * @return the set of keys in the multimap (the returned set might be
     * modifiable but it has no effect on the multimap)
     */
    Set<K> keySet();

    /**
     * Returns the collection of values in the multimap.
     * <p>
     * <b>Warning:</b> The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection,
     * and vice-versa.
     *
     * @return the collection of values in the multimap (the returned
     * collection might be modifiable but it has no effect on the multimap)
     */
    Collection<V> values();

    /**
     * Returns the set of key-value pairs in the multimap.
     * <p>
     * <b>Warning:</b> The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection,
     * and vice-versa.
     *
     * @return the set of key-value pairs in the multimap (the returned
     * set might be modifiable but it has no effect on the multimap)
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Returns whether the multimap contains an entry with the key.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key the key whose existence is checked
     * @return {@code true} if the multimap contains an entry with the key,
     * {@code false} otherwise
     */
    boolean containsKey(K key);

    /**
     * Returns whether the multimap contains an entry with the value.
     *
     * @param value the value whose existence is checked
     * @return {@code true} if the multimap contains an entry with the value,
     * {@code false} otherwise.
     */
    boolean containsValue(Object value);

    /**
     * Returns whether the multimap contains the given key-value pair.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key   the key whose existence is checked
     * @param value the value whose existence is checked
     * @return {@code true} if the multimap contains the key-value pair,
     * {@code false} otherwise
     */
    boolean containsEntry(K key, V value);

    /**
     * Returns the number of key-value pairs in the multimap.
     *
     * @return the number of key-value pairs in the multimap
     */
    int size();

    /**
     * Clears the multimap. Removes all key-value pairs.
     */
    void clear();

    /**
     * Returns the number of values that match the given key in the multimap.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key the key whose values count is to be returned
     * @return the number of values that match the given key in the multimap
     */
    int valueCount(K key);

    /**
     * Adds a local entry listener for this multimap.
     * <p>
     * The added listener will be only listening for the events
     * (add/remove/update) of the locally owned entries.
     * <p>
     * Note that entries in distributed multimap are partitioned across the
     * cluster members; each member owns and manages some portion of the
     * entries. Owned entries are called local entries. This listener will be
     * listening for the events of local entries.
     * <p>
     * For example your cluster has member1 and member2. On member2 you added a
     * local listener, and from member1, you call {@code multimap.put(key2, value2)}.
     * If the key2 is owned by member2, then the local listener will be notified
     * for the add/update event. Also note that entries can migrate to other
     * nodes for load balancing and/or membership change.
     *
     * @param listener entry listener for this multimap
     * @return returns registration ID for the entry listener
     * @see #localKeySet()
     */
    String addLocalEntryListener(EntryListener<K, V> listener);

    /**
     * Adds an entry listener for this multimap.
     * <p>
     * The listener will be notified for all multimap
     * add/remove/update/evict events.
     *
     * @param listener     entry listener for this multimap
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value,
     *                     {@code false} otherwise
     * @return returns registration ID for the entry listener
     */
    String addEntryListener(EntryListener<K, V> listener, boolean includeValue);

    /**
     * Removes the specified entry listener.
     * <p>
     * Returns silently if no such listener was added before.
     *
     * @param registrationId registration ID of listener
     * @return true if registration is removed, false otherwise
     */
    boolean removeEntryListener(String registrationId);

    /**
     * Adds the specified entry listener for the specified key.
     * <p>
     * The listener will be notified for all add/remove/update/evict events for
     * the specified key only.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param listener     the entry listener
     * @param key          the key to listen to
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value,
     *                     {@code false} otherwise
     * @return returns registration ID
     */
    String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue);

    /**
     * Acquires a lock for the specified key.
     * <p>
     * If the lock is not available, then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has
     * been acquired.
     * <p>
     * The scope of the lock is for this multimap only. The acquired lock is
     * only for the key in this multimap.
     * <p>
     * Locks are re-entrant, so if the key is locked N times, then it should
     * be unlocked N times before another thread can acquire it.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key the key to lock
     */
    void lock(K key);

    /**
     * Acquires the lock for the specified key for the specified lease time.
     * <p>
     * After the lease time, the lock will be released.
     * <p>
     * If the lock is not available, then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * <p>
     * Scope of the lock is for this map only.The acquired lock is only for
     * the key in this map.
     * <p>
     * Locks are re-entrant, so if the key is locked N times, then it should
     * be unlocked N times before another thread can acquire it.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key       the key to lock
     * @param leaseTime time to wait before releasing the lock
     * @param timeUnit  unit of time for the lease time
     */
    void lock(K key, long leaseTime, TimeUnit timeUnit);

    /**
     * Checks the lock for the specified key.
     * <p>
     * If the lock is acquired, this method returns {@code true}, else it
     * returns {@code false}.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key key to lock to be checked.
     * @return {@code true} if the lock is acquired, {@code false} otherwise.
     */
    boolean isLocked(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>
     * If the lock is not available, then the current thread does not wait and
     * the method returns false immediately.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key the key to lock.
     * @return {@code true} if lock is acquired, {@code false} otherwise
     */
    boolean tryLock(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>
     * If the lock is not available, then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until one of two things
     * happens:
     * <ul>
     * <li>the lock is acquired by the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param time     the maximum time to wait for the lock
     * @param timeunit the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired, {@code false} if the
     * waiting time elapsed before the lock was acquired
     */
    boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException;

    /**
     * Tries to acquire the lock for the specified key for the specified
     * lease time.
     * <p>
     * After lease time, the lock will be released.
     * <p>
     * If the lock is not available, then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until one of two things
     * happens:
     * <ul>
     * <li>the lock is acquired by the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key           key to lock in this map
     * @param time          maximum time to wait for the lock
     * @param timeunit      time unit of the {@code time} argument
     * @param leaseTime     time to wait before releasing the lock
     * @param leaseTimeunit unit of time to specify lease time
     * @return {@code true} if the lock was acquired and {@code false} if the
     * waiting time elapsed before the lock was acquired
     * @throws NullPointerException if the specified key is {@code null}
     */
    boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeunit) throws InterruptedException;

    /**
     * Releases the lock for the specified key.
     * <p>
     * It never blocks and returns immediately.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key the key to lock
     */
    void unlock(K key);

    /**
     * Releases the lock for the specified key regardless of the lock owner.
     * <p>
     * It always successfully unlocks the key, never blocks and returns immediately.
     * <p>
     * <b>Warning:</b> This method uses {@code hashCode} and {@code equals} of
     * the binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     *
     * @param key key to lock
     */
    void forceUnlock(K key);

    /**
     * Returns {@code LocalMultiMapStats} for this map.
     * <p>
     * {@code LocalMultiMapStats} is the statistics for the local portion of this
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
     * Executes a predefined aggregation on the multimaps data set.
     * <p>
     * The {@link com.hazelcast.mapreduce.aggregation.Supplier} is used to
     * either select or to select and extract a (sub-)value.
     * A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     * <p>
     * Method does not honor Quorum.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the multimap
     * @param aggregation     the aggregation that is being executed against the multimap
     * @param <SuppliedValue> the final type emitted from the supplier
     * @param <Result>        the resulting aggregation value type
     * @return Returns the aggregated value for the multimaps data set
     * @deprecated The old Aggregations API is superseded by Fast Aggregations ({@link com.hazelcast.aggregation})
     * which does not yet support MultiMap. Hazelcast Jet can be used in the future to replace this functionality.
     */
    @Deprecated
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation);

    /**
     * Executes a predefined aggregation on the multimaps data set.
     * <p>
     * The {@link com.hazelcast.mapreduce.aggregation.Supplier} is used to
     * either select, or to select and extract, a (sub-)value.
     * A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     * <p>
     * Method does not honor Quorum.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the multimap
     * @param aggregation     the aggregation that is being executed against the multimap
     * @param jobTracker      the {@link com.hazelcast.mapreduce.JobTracker} instance to execute the aggregation
     * @param <SuppliedValue> the final type emitted from the supplier
     * @param <Result>        the resulting aggregation value type
     * @return Returns the aggregated value
     * @deprecated The old Aggregations API is superseded by Fast Aggregations ({@link com.hazelcast.aggregation})
     * which does not yet support MultiMap. Hazelcast Jet can be used in the future to replace this functionality.
     */
    @Deprecated
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation,
                                             JobTracker jobTracker);
}

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

package com.hazelcast.core;

import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.query.Predicate;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * <p>A ReplicatedMap is a map-like data structure with weak consistency
 * and values locally stored on every node of the cluster. </p>
 * <p>Whenever a value is written asynchronously, the new value will be internally
 * distributed to all existing cluster members, and eventually every node will have
 * the new value.</p>
 * <p>When a new node joins the cluster, the new node initially will request existing
 * values from older nodes and replicate them locally.</p>
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @since 3.2
 */
public interface ReplicatedMap<K, V> extends Map<K, V>, DistributedObject {

    /**
     * <p>Associates a given value to the specified key and replicates it to the
     * cluster. If there is an old value, it will be replaced by the specified
     * one and returned from the call.</p>
     * <p>In addition, you have to specify a ttl and its {@link TimeUnit}
     * to define when the value is outdated and thus should be removed from the
     * replicated map.</p>
     *
     * @param key      key with which the specified value is to be associated.
     * @param value    value to be associated with the specified key.
     * @param ttl      ttl to be associated with the specified key-value pair.
     * @param timeUnit TimeUnit to be used for the ttl value.
     */
    V put(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * <p>The clear operation wipes data out of the replicated maps.
     * It is the only synchronous remote operation in this implementation, so
     * be aware that this might be a slow operation.</p>
     * <p>If some node fails on executing the operation, it is retried for at most
     * 3 times (on the failing nodes only). If it does not work after the third time, this
     * method throws a {@link com.hazelcast.core.OperationTimeoutException} back
     * to the caller.</p>
     *
     * @throws com.hazelcast.core.OperationTimeoutException thrown if clear could not
     *                                           be executed on remote nodes.
     */
    void clear();

    /**
     * Removes the specified entry listener.
     * Returns silently if there was no such listener added before.
     *
     * @param id id of the registered entry listener.
     * @return true if registration is removed, false otherwise.
     */
    boolean removeEntryListener(String id);

    /**
     * Adds an entry listener for this map. The listener will be notified
     * for all map add/remove/update/evict events.
     *
     * @param listener entry listener
     */
    String addEntryListener(EntryListener<K, V> listener);

    /**
     * Adds the specified entry listener for the specified key.
     * The listener will be notified for all
     * add/remove/update/evict events of the specified key only.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param listener the entry listener to add
     * @param key      the key to listen to
     * @throws NullPointerException if the specified key is null
     */
    String addEntryListener(EntryListener<K, V> listener, K key);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener  the entry listener to add
     * @param predicate the predicate for filtering entries
     */
    String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener  the entry listener
     * @param predicate the predicate for filtering entries
     * @param key       the key to listen to
     */
    String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key);

    /**
     * Returns a lazy {@link Collection} view of the values contained in this map.<br/>
     * A LazyCollection is optimized for querying speed (preventing eager deserialization
     * and hashing on HashSet insertion) and does <b>NOT</b> provide all operations.
     * Any kind of mutating function will throw an
     * {@link java.lang.UnsupportedOperationException}. Same is true for operations
     * like {@link java.util.Collection#contains(Object)} and
     * {@link java.util.Collection#containsAll(java.util.Collection)}, which would result in
     * very poor performance if called repeatedly (for example, in a loop). If the use
     * case is different from querying the data, please copy the resulting set into a
     * new {@link java.util.List} or similar data structure.
     * <pre>
     *     ReplicatedMap&lt;K, V> repMap = ...;
     *     // Returns a LazyCollection
     *     Collection&lt;V> values = repMap.values();
     *     List&lt;V> copy = new ArrayList&lt;V>(values);
     *     if (copy.containsAll(possibleValues)) {
     *         // ...
     *     }
     * </pre>
     * Due to the lazy nature of the returned set, changes to the map (addition,
     * removal, update) might be reflected in the set.<br/>
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior. If a specific order is needed, use
     * {@link #values(java.util.Comparator)} to force reordering of the
     * elements before returning.<br/>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @return A collection view of the values contained in this map.
     */
    Collection<V> values();

    /**
     * Returns an eagerly populated {@link Collection} view of the values contained in this map.
     * The collection is <b>NOT</b> backed by the map, so changes to the map are
     * <b>NOT</b> reflected in the collection, and vice-versa.<br/>
     * The order of the elements is guaranteed by executing the given
     * {@link java.util.Comparator} before returning the elements.<br/>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @param comparator the Comparator to sort the returned elements.
     * @return An eagerly populated {@link Collection} view of the values contained in this map.
     */
    Collection<V> values(Comparator<V> comparator);

    /**
     * Returns a lazy {@link Set} view of the mappings contained in this map.<br/>
     * A LazySet is optimized for querying speed (preventing eager deserialization
     * and hashing on HashSet insertion) and does <b>NOT</b> provide all operations.
     * Any kind of mutating function will throw an
     * {@link java.lang.UnsupportedOperationException}. Same is true for operations
     * like {@link java.util.Set#contains(Object)} and
     * {@link java.util.Set#containsAll(java.util.Collection)} which would result in
     * very poor performance if called repeatedly (for example, in a loop). If the use
     * case is different from querying the data, please copy the resulting set into a
     * new {@link java.util.HashSet}.
     * <pre>
     *     ReplicatedMap&lt;K, V> repMap = ...;
     *     // Returns a LazySet
     *     Set&lt;Map.Entry&lt;K, V>> entrySet = repMap.entrySet();
     *     Set&lt;Map.Entry&lt;K, V>> copy = new HashSet&lt;Map.Entry&lt;K, V>>(entrySet);
     * </pre>
     * Due to the lazy nature of the returned set, changes to the map (addition,
     * removal, update) might be reflected in the set.<br/>
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior.<br/>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @return A lazy set view of the mappings contained in this map.
     */
    Set<Entry<K, V>> entrySet();

    /**
     * Returns a lazy {@link Set} view of the key contained in this map.<br/>
     * A LazySet is optimized for querying speed (preventing eager deserialization
     * and hashing on HashSet insertion) and does <b>NOT</b> provide all operations.
     * Any kind of mutating function will throw an
     * {@link java.lang.UnsupportedOperationException}. Same is true for operations
     * like {@link java.util.Set#contains(Object)} and
     * {@link java.util.Set#containsAll(java.util.Collection)} which would result in
     * very poor performance if called repeatedly (for example, in a loop). If the use
     * case is different from querying the data, please copy the resulting set into a
     * new {@link java.util.HashSet}.
     * <pre>
     *     ReplicatedMap&lt;K, V> repMap = ...;
     *     // Returns a LazySet
     *     Set&lt;K> keySet = repMap.keySet();
     *     Set&lt;K> copy = new HashSet&lt;K>(keySet);
     *     for (K key : possibleKeys) {
     *       if (!copy.contains(key))
     *         return false;
     *     }
     * </pre>
     * <p/>
     * Due to the lazy nature of the returned set, changes to the map (addition,
     * removal, update) might be reflected in the set.<br/>
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior.<br/>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @return A lazy {@link Set} view of the keys contained in this map.
     */
    Set<K> keySet();

    /**
     * Returns LocalReplicatedMapStats for this replicated map.
     * LocalReplicatedMapStats is the statistics for the local
     * replicated map and contains information such as getCount
     * putCount, hits, lastUpdateTime etc.
     * <p/>
     *
     * @return this replicated map's statistics.
     */
    LocalReplicatedMapStats getReplicatedMapStats();

}

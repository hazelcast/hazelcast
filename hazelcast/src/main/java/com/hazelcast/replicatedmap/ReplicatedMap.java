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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * <p>A ReplicatedMap is a map data structure with weak consistency
 * and has entries stored locally on every node of the cluster.
 *
 * <p>Whenever a value is written, the new value will be internally
 * distributed to all existing cluster members, and eventually every node
 * will have the new value. Reading is always local on members (except
 * for lite members).
 *
 * <p>When a new node joins the cluster, it initially requests existing
 * values from older nodes and replicates them locally.
 *
 * <p>Supports split-brain protection {@link SplitBrainProtectionConfig} since 3.10
 * in cluster versions 3.10 and higher.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @since 3.2
 */
public interface ReplicatedMap<K, V> extends Map<K, V>, DistributedObject {

    /**
     * <p>Associates a given value to the specified key and replicates it to the
     * cluster. If there is an old value, it will be replaced by the specified
     * one and returned from the call.
     * <p>In addition, you have to specify a ttl and its {@link TimeUnit}
     * to define when the value is outdated and thus should be removed from the
     * replicated map.
     *
     * @param key      key with which the specified value is to be associated.
     * @param value    value to be associated with the specified key.
     * @param ttl      ttl to be associated with the specified key-value pair.
     * @param timeUnit TimeUnit to be used for the ttl value.
     */
    V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeUnit);

    /**
     * <p>The clear operation wipes data out of the replicated maps.
     * <p>If some node fails on executing the operation, it is retried for at most
     * 5 times (on the failing nodes only).
     */
    void clear();

    /**
     * Removes the specified entry listener.
     * Returns silently if there was no such listener added before.
     *
     * @param id ID of the registered entry listener.
     * @return true if registration is removed, false otherwise.
     */
    boolean removeEntryListener(@Nonnull UUID id);

    /**
     * Adds an entry listener for this map. The listener will be notified
     * for all map add/remove/update/evict events.
     *
     * @param listener entry listener
     */
    @Nonnull
    UUID addEntryListener(@Nonnull EntryListener<K, V> listener);

    /**
     * Adds the specified entry listener for the specified key.
     * The listener will be notified for all
     * add/remove/update/evict events of the specified key only.
     * <p><b>Warning:</b>
     * This method uses <code>hashCode</code> and <code>equals</code> of the binary form of
     * the <code>key</code>, not the actual implementations of <code>hashCode</code> and <code>equals</code>
     * defined in the <code>key</code>'s class.
     *
     * @param listener the entry listener to add
     * @param key      the key to listen to
     * @throws NullPointerException if the specified key is null
     */
    @Nonnull
    UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nullable K key);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener  the entry listener to add
     * @param predicate the predicate for filtering entries
     */
    @Nonnull
    UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nonnull Predicate<K, V> predicate);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener  the entry listener
     * @param predicate the predicate for filtering entries
     * @param key       the key to listen to
     */
    @Nonnull
    UUID addEntryListener(@Nonnull EntryListener<K, V> listener,
                          @Nonnull Predicate<K, V> predicate,
                          @Nullable K key);

    /**
     * Returns a lazy {@link Collection} view of the values contained in this map.<br>
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
     *     ReplicatedMap&lt;K, V&gt; repMap = ...;
     *     // Returns a LazyCollection
     *     Collection&lt;V&gt; values = repMap.values();
     *     List&lt;V&gt; copy = new ArrayList&lt;V&gt;(values);
     *     if (copy.containsAll(possibleValues)) {
     *         // ...
     *     }
     * </pre>
     * Due to the lazy nature of the returned set, changes to the map (addition,
     * removal, update) might be reflected on the collection.<br>
     * Changes on the map are <b>NOT</b> reflected on the collection on the <b>CLIENT</b> or vice versa.
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior. If a specific order is needed, use
     * {@link #values(java.util.Comparator)} to force reordering of the
     * elements before returning.<br>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @return A collection view of the values contained in this map.
     */
    @Nonnull
    Collection<V> values();

    /**
     * Returns an eagerly populated {@link Collection} view of the values contained in this map.
     * The collection is <b>NOT</b> backed by the map, so changes to the map are
     * <b>NOT</b> reflected on the collection, and vice-versa.<br>
     * The order of the elements is guaranteed by executing the given
     * {@link java.util.Comparator} before returning the elements.<br>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @param comparator the Comparator to sort the returned elements.
     * @return An eagerly populated {@link Collection} view of the values contained in this map.
     */
    @Nonnull
    Collection<V> values(@Nullable Comparator<V> comparator);

    /**
     * Returns a lazy {@link Set} view of the mappings contained in this map.<br>
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
     *     ReplicatedMap&lt;K, V&gt; repMap = ...;
     *     // Returns a LazySet
     *     Set&lt;Map.Entry&lt;K, V&gt;&gt; entrySet = repMap.entrySet();
     *     Set&lt;Map.Entry&lt;K, V&gt;&gt; copy = new HashSet&lt;Map.Entry&lt;K, V&gt;&gt;(entrySet);
     * </pre>
     * Due to the lazy nature of the returned set, changes to the map (addition,
     * removal, update) might be reflected on the set.<br>
     * Changes on the map are <b>NOT</b> reflected on the set on the <b>CLIENT</b> or vice versa.
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior.<br>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @return A lazy set view of the mappings contained in this map.
     */
    @Nonnull
    Set<Entry<K, V>> entrySet();

    /**
     * Returns a lazy {@link Set} view of the key contained in this map.<br>
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
     *     ReplicatedMap&lt;K, V&gt; repMap = ...;
     *     // Returns a LazySet
     *     Set&lt;K&gt; keySet = repMap.keySet();
     *     Set&lt;K&gt; copy = new HashSet&lt;K&gt;(keySet);
     *     for (K key : possibleKeys) {
     *       if (!copy.contains(key))
     *         return false;
     *     }
     * </pre>
     * <p>
     * Due to the lazy nature of the returned set, changes to the map (addition,
     * removal, update) might be reflected on the set.<br>
     * Changes on the map are <b>NOT</b> reflected on the set on the <b>CLIENT</b> or vice versa.
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior.<br>
     * Changes to any returned object are <b>NOT</b> replicated back to other
     * members.
     *
     * @return A lazy {@link Set} view of the keys contained in this map.
     */
    @Nonnull
    Set<K> keySet();

    /**
     * Returns LocalReplicatedMapStats for this replicated map.
     * LocalReplicatedMapStats is the statistics for the local
     * replicated map and contains information such as getCount
     * putCount, hits, lastUpdateTime etc.
     * <p>
     *
     * @return this replicated map's statistics.
     */
    @Nonnull
    LocalReplicatedMapStats getReplicatedMapStats();

}

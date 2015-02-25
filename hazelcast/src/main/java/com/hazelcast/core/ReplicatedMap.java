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

import com.hazelcast.query.Predicate;
import com.hazelcast.spi.annotation.Beta;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * <p>A ReplicatedMap is a map-like data structure with non-strong consistency
 * (so-called eventually consistent) and values locally stored on every
 * node of the cluster. When accessing values, due to the eventually
 * consistency, it is possible to read stale data since replication
 * is not handled in a synchronous way.</p>
 * <p>Whenever a value is written asynchronously, the new value will be internally
 * distributed to all existing cluster members, and eventually every node will have
 * the new value and the cluster again is in a consistent state.</p>
 * <p>When a new node joins the cluster, the new node initially will request existing
 * values from older nodes and replicate them locally.</p>
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @since 3.2
 */
@Beta
public interface ReplicatedMap<K, V>
        extends Map<K, V>, DistributedObject {

    /**
     * <p>Associates a given value to the specified key and replicates it to the
     * cluster. If there is an old value, it will be replaced by the specified
     * one and returned from the call.</p>
     * <p>In addition, you have to specify a ttl and its {@link TimeUnit}
     * to define when the value is outdated and thus should be removed from the
     * replicated map.</p>
     *
     * @param key      key with which the specified value is to be associated
     * @param value    value to be associated with the specified key
     * @param ttl      ttl to be associated with the specified key-value pair
     * @param timeUnit TimeUnit to be used for the ttl value
     */
    V put(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * <p>The clear operation is used for wiping data out of the replicated maps.
     * It is the only synchronous remote operation in this implementation, so
     * be aware that this might be a slow operation.</p>
     * <p>If some node fails on executing the operation, it is retried for at most
     * 3 times (on the failing nodes only). If not working after the third time, this
     * method throws a {@link com.hazelcast.core.OperationTimeoutException} back
     * to the caller.</p>
     *
     * @throws com.hazelcast.core.OperationTimeoutException thrown if clear could not
     *                                                          executed on remote nodes
     */
    void clear();

    /**
     * Removes the specified entry listener.
     * Returns silently if there was no such listener added before.
     *
     * @param id id of the registered listener
     * @return true if registration is removed, false otherwise
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
     * @param listener the entry listener
     * @param key      the key to listen to
     * @throws NullPointerException if the specified key is null
     */
    String addEntryListener(EntryListener<K, V> listener, K key);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener  the entry listener
     * @param predicate the predicate for filtering entries
     */
    String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate);

    /**
     * Adds an continuous entry listener for this map. Listener will get notified
     * for map add/remove/update/evict events filtered by given predicate.
     *
     * @param listener  the entry listener
     * @param predicate the predicate for filtering entries
     * @param key       the key to listen to
     */
    String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key);

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is <b>NOT</b> backed by the map, so changes to the map are
     * <b>NOT</b> reflected in the collection, and vice-versa.<br/>
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior. If a specific order is needed, use
     * {@link #values(java.util.Comparator)} to force reordering of the
     * elements before returning.
     *
     * @return a collection view of the values contained in this map
     */
    Collection<V> values();

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is <b>NOT</b> backed by the map, so changes to the map are
     * <b>NOT</b> reflected in the collection, and vice-versa.<br/>
     * The order of the elements is guaranteed by executing the given
     * {@link java.util.Comparator} before returning the elements.
     *
     * @param comparator the Comparator to sort the returned elements
     * @return a collection view of the values contained in this map
     */
    Collection<V> values(Comparator<V> comparator);

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is <b>NOT</b> backed by the map, so changes to the map are
     * <b>NOT</b> reflected in the set, and vice-versa.<br/>
     * The order of the elements is not guaranteed due to the internal
     * asynchronous replication behavior.
     *
     * @return a set view of the mappings contained in this map
     */
    Set<Entry<K, V>> entrySet();

}

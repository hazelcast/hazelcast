/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Concurrent, distributed, observable and queryable map.
 * <p>
 * <b>This class is <i>not</i> a general-purpose {@code ConcurrentMap}
 * implementation! While this class implements the {@code Map} interface,
 * it intentionally violates {@code Map's} general contract, which mandates
 * the use of the {@code equals} method when comparing objects. Instead of
 * the {@code equals} method, this implementation compares the serialized
 * byte version of the objects.</b>
 * <p>
 * <b>Moreover, stored values are handled as having a value type semantics,
 * while standard Java implementations treat them as having a reference type
 * semantics.</b>
 * <p>
 * <b>Gotchas:</b>
 * <ul>
 * <li>Methods, including but not limited to {@code get}, {@code containsKey},
 * {@code containsValue}, {@code evict}, {@code remove}, {@code put},
 * {@code putIfAbsent}, {@code replace}, {@code lock}, {@code unlock}, do
 * not use {@code hashCode} and {@code equals} implementations of keys.
 * Instead, they use {@code hashCode} and {@code equals} of binary (serialized)
 * forms of the objects.</li>
 * <li>The {@code get} method returns a clone of original values, so modifying
 * the returned value does not change the actual value in the map. You should
 * put the modified value back to make changes visible to all nodes.
 * For additional info, see {@link IMap#get(Object)}.</li>
 * <li>Methods, including but not limited to {@code keySet}, {@code values},
 * {@code entrySet}, return an <b>immutable</b> collection clone of the values.
 * The collection is <b>NOT</b> backed by the map, so changes to the map are
 * <b>NOT</b> reflected in the collection.</li>
 * <li>Be careful while using default interface method implementations from
 * {@link ConcurrentMap} and {@link Map}. Under the hood they are typically
 * implemented as a sequence of more primitive map operations, therefore the
 * operations won't be executed atomically.</li>
 * </ul>
 * <p>
 * This class does <em>not</em> allow {@code null} to be used as a key or
 * value.
 * <p>
 * <b>Entry Processing</b>
 * <p>
 * The following operations are lock-aware, since they operate on a single
 * key only.
 * If the key is locked, the EntryProcessor will wait until it acquires the
 * lock.
 * <ul>
 * <li>{@link IMap#executeOnKey(Object, EntryProcessor)}</li>
 * <li>{@link IMap#submitToKey(Object, EntryProcessor)}</li>
 * </ul>
 * However, there are following methods that run the {@code EntryProcessor}
 * on more than one entry.
 * These operations are not lock-aware.
 * The {@code EntryProcessor} will process the entries no matter if they
 * are locked or not.
 * The user may however check if an entry is locked by casting the
 * {@link Map.Entry} to {@link LockAware} and invoking the
 * {@link LockAware#isLocked()} method.
 * <ul>
 * <li>{@link IMap#executeOnEntries(EntryProcessor)}</li>
 * <li>{@link IMap#executeOnEntries(EntryProcessor, Predicate)}</li>
 * <li>{@link IMap#executeOnKeys(Set, EntryProcessor)}</li>
 * </ul>
 * This applies to both {@code EntryProcessor} and {@code BackupEntryProcessor}.
 *
 * <p>
 * <b>Split-brain</b>
 * <p>
 * Behaviour of {@link IMap} under split-brain scenarios should be taken
 * into account when using this data structure.  During a split, each
 * partitioned cluster will either create a brand new {@link IMap} or it
 * will continue to use the primary or back-up version.
 * <p>
 * When the split heals, Hazelcast by default, performs a
 * {@link com.hazelcast.spi.merge.PutIfAbsentMergePolicy}.
 * Users can also decide to
 * <a href="https://docs.hazelcast.com/hazelcast/latest/network-partitioning/split-brain-recovery#custom-merge-policies">
 * specify their own map merge policies</a>, these policies when used in
 * concert with
 * <a href="http://hal.upmc.fr/inria-00555588/document">CRDTs (Convergent and Commutative
 * Replicated Data Types)</a> can ensure against data loss during a split-brain.
 * <p>
 * As a defensive mechanism against such inconsistency, consider using the
 * in-built
 * <a href="https://docs.hazelcast.com/hazelcast/latest/network-partitioning/split-brain-protection">
 * split-brain protection for {@link IMap}</a>.  Using this functionality
 * it is possible to restrict operations in smaller partitioned clusters.
 * It should be noted that there is still an inconsistency window between
 * the time of the split and the actual detection. Therefore, using this
 * reduces the window of inconsistency but can never completely eliminate
 * it.
 *
 * <p><b>Interactions with the map store</b>
 * <p>
 * Maps can be configured to be backed by a map store to persist the entries.
 * In this case many of the IMap methods call {@link MapLoader} or
 * {@link MapStore} methods to load, store or remove data. Each method's
 * javadoc describes the way of its interaction with the map store.
 *
 * <p><b>Expiration and eviction</b>
 * <p>
 * Expiration puts a limit on the maximum lifetime of an entry stored
 * inside the map. When the entry expires it can't be retrieved from the map
 * any longer and at some point in time it will be cleaned out from the map
 * to free up the memory. There are two expiration policies:
 * <ul>
 * <li>The time-to-live (TTL) expiration policy limits the lifetime of the
 * entry relative to the time of the last <i>write</i> access performed on
 * the entry. The default TTL value for the map may be configured using
 * the {@code time-to-live-seconds} setting, which has an infinite by default.
 * An individual entry may have its own TTL value assigned using one of the
 * methods accepting a TTL value, for instance using the
 * {@link #put(Object, Object, long, TimeUnit) put} method. If there is no
 * TTL value provided for the individual entry, it inherits the value set
 * in the map configuration.
 * <li>The max-idle expiration policy limits the lifetime of the entry
 * relative to the time of the last <i>read</i> or <i>write</i> access
 * performed on the entry. The max-idle value for the map may be configured
 * using the {@code max-idle-seconds} setting, which has an infinite value
 * by default.
 * </ul>
 * <p>
 * Both expiration policies may be used simultaneously on the map entries.
 * In such case, the entry is considered expired if at least one of the
 * policies marks it as expired.
 * <p>
 * Eviction puts a limit on the maximum size of the map. If the size of the
 * map grows larger than the maximum allowed size, an eviction policy decides
 * which item to evict from the map to reduce its size. The maximum allowed
 * size may be configured using the
 * {@link MaxSizePolicy max-size} setting
 * and the eviction policy may be configured using the
 * {@link com.hazelcast.config.EvictionPolicy eviction-policy} setting as well.
 * By default, maps have no restrictions on the size and may grow arbitrarily
 * large.
 * <p>
 * Eviction may be enabled along with the expiration policies. In such case,
 * the expiration policies continue to work as usual cleaning out the expired
 * entries regardless of the map size.
 * <p>
 * Locked map entries are not the subjects for the expiration and eviction
 * policies.
 *
 * <p><b>Mutating methods without TTL</b>
 * <p>
 * Certain {@link IMap} methods perform the entry set mutation and don't
 * accept TTL as a parameter. Entries created or updated by such methods are
 * subjects for the following TTL calculation procedure:
 * <ul>
 * <li>If the entry is new, i.e. the entry was created, it receives the default
 * TTL value configured for the map using the {@code time-to-live-seconds}
 * configuration setting. If this setting is not provided for the map, the
 * entry receives an infinite TTL value.
 * <li>If the entry already exists, i.e. the entry was updated, its TTL value
 * remains unchanged and its lifetime is prolonged by this TTL value.
 * </ul>
 * The methods to which this procedure applies: {@link #put(Object, Object) put},
 * {@link #set(Object, Object) set}, {@link #putAsync(Object, Object) putAsync},
 * {@link #setAsync(Object, Object) setAsync},
 * {@link #tryPut(Object, Object, long, TimeUnit) tryPut},
 * {@link #putAll(Map) putAll}, {@link #setAll(Map) setAll},
 * {@link #putAllAsync(Map) putAllAsync}, {@link #setAllAsync(Map) setAllAsync},
 * {@link #replace(Object, Object, Object)} and {@link #replace(Object, Object)}.
 *
 *  <p>
 *  <b>Asynchronous methods</b>
 *  <p>
 *  Asynchronous methods return a {@link CompletionStage} that can be used to
 *  chain further computation stages. Alternatively, a {@link java.util.concurrent.CompletableFuture}
 *  can be obtained via {@link CompletionStage#toCompletableFuture()} to wait
 *  for the operation to complete in a blocking way.
 *  <p>
 *  Actions supplied for dependent completions of default non-async methods and async methods
 *  without an explicit {@link java.util.concurrent.Executor} argument are performed
 *  by the {@link java.util.concurrent.ForkJoinPool#commonPool()} (unless it does not
 *  support a parallelism level of at least 2, in which case a new {@code Thread} is
 *  created per task).
 *
 * @param <K> key type
 * @param <V> value type
 * @see java.util.concurrent.ConcurrentMap
 */
@SuppressWarnings("MethodCount")
public interface IMap<K, V> extends ConcurrentMap<K, V>, BaseMap<K, V>, Iterable<Map.Entry<K, V>> {

    /**
     * {@inheritDoc}
     * <p>
     * No atomicity guarantees are given. It could be that in case of failure
     * some of the key/value-pairs get written, while others are not.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * For each element not found in memory
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map, which may come at a significant
     * performance cost. Exceptions thrown by load fail the operation
     * and are propagated to the caller. The elements which were added
     * before the exception was thrown will remain in the map, the rest
     * will not be added.
     * <p>
     * If write-through persistence mode is configured,
     * {@link MapStore#store(Object, Object)} is invoked for each element
     * before the element is added in memory, which may come at a
     * significant performance cost. Exceptions thrown by store fail the
     * operation and are propagated to the caller. The elements which
     * were added before the exception was thrown will remain in the map,
     * the rest will not be added.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     */
    @Override
    void putAll(@Nonnull Map<? extends K, ? extends V> m);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If {@code key} is not found in memory
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     *
     * @throws NullPointerException if the specified key is {@code null}
     */
    @Override
    boolean containsKey(@Nonnull Object key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified value is {@code null}
     */
    @Override
    boolean containsValue(@Nonnull Object value);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method returns a clone of the original value, so modifying the returned
     * value does not change the actual value in the map. You should put the
     * modified value back to make changes visible to all nodes.
     * <pre>
     *      V value = map.get(key);
     *      value.updateSomeProperty();
     *      map.put(key, value);
     * </pre>
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If value with {@code key} is not found in memory
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     *
     * @throws NullPointerException if the specified key is {@code null}
     */
    @Override
    V get(@Nonnull Object key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p><b>Note:</b>
     * Use {@link #set(Object, Object)} if you don't need the return value, it's
     * slightly more efficient.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    V put(@Nonnull K key, @Nonnull V value);

    /**
     * Removes the mapping for a key from this map if it is present.
     * <p>
     * If you don't need the previously mapped value for the removed key, prefer
     * to use {@link #delete} and avoid the cost of serialization and network
     * transfer.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is removed from the memory, {@link MapStore#delete(Object)} is
     * called to remove the value from the map store. Exceptions thrown
     * by delete fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @throws NullPointerException if the specified key is null
     * @see #delete(Object)
     */
    @Override
    V remove(@Nonnull Object key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is removed from the memory, {@link MapStore#delete(Object)} is
     * called to remove the value from the map store. Exceptions thrown
     * by delete fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    boolean remove(@Nonnull Object key, @Nonnull Object value);

    /**
     * Removes all entries which match with the supplied predicate.
     * <p>
     * If this map has index, matching entries will be found via
     * index search, otherwise they will be found by full-scan.
     * <p>
     * Note that calling this method also removes all entries from
     * caller's Near Cache.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before a value is
     * removed from the memory, {@link MapStore#delete(Object)} is called to
     * remove the value from the map store. Exceptions thrown by delete fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with write-coalescing
     * turned off, {@link com.hazelcast.map.ReachedMaxSizeException} may be
     * thrown if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param predicate matching entries with this predicate will be removed
     *                  from this map
     * @throws NullPointerException     if the specified predicate is null
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     */
    void removeAll(@Nonnull Predicate<K, V> predicate);

    /**
     * Removes the mapping for the key from this map if it is present.
     * <p>
     * Unlike {@link #remove(Object)}, this operation does not return
     * the removed value, which avoids the serialization and network transfer cost of the
     * returned value. If the removed value will not be used, this operation
     * is preferred over the remove operation for better performance.
     * <p>
     * The map will not contain a mapping for the specified key once the call returns.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method breaks the contract of EntryListener.
     * When an entry is removed by delete(), it fires an EntryEvent with a null oldValue.
     * <p>
     * Also, a listener with predicates will have null values, so only keys can be queried via predicates.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is removed from the memory, {@link MapStore#delete(Object)}
     * is called to remove the value from the map store. Exceptions
     * thrown by delete fail the operation and are propagated to the
     * caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key key whose mapping is to be removed from the map
     * @throws ClassCastException   if the key is of an inappropriate type for this map (optional)
     * @throws NullPointerException if the specified key is null
     * @see #remove(Object)
     */
    @Override
    void delete(@Nonnull Object key);

    /**
     * If this map has a MapStore, this method flushes
     * all the local dirty entries.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * Calls {@link MapStore#storeAll(Map)} and/or
     * {@link MapStore#deleteAll(Collection)} with elements marked dirty.
     * <p>
     * Please note that this method has effect only if write-behind
     * persistence mode is configured. If the persistence mode is
     * write-through calling this method has no practical effect, but an
     * operation is executed on all partitions wasting resources.
     */
    void flush();

    /**
     * Returns an immutable map of entries for the given keys.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * The returned map is <b>NOT</b> backed by the original map, so
     * changes to the original map are <b>NOT</b> reflected in the
     * returned map.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code keys}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If any keys are not found in memory, {@link MapLoader#loadAll}
     * is called with the missing keys. Exceptions thrown by
     * loadAll fail the operation and are propagated to the caller.
     *
     * @param keys keys to get (keys inside the collection cannot be null)
     * @return an immutable map of entries
     * @throws NullPointerException if any of the specified
     *                              keys are null or if any key or any value returned
     *                              from {@link MapLoader#loadAll} is {@code null}.
     */
    Map<K, V> getAll(@Nullable Set<K> keys);

    /**
     * Loads all keys into the store. This is a batch load operation so
     * that an implementation can optimize multiple loads.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * Calls {@link MapLoader#loadAllKeys()} and with the loaded keys
     * calls {@link MapLoader#loadAll(java.util.Collection)} on each
     * partition. Exceptions thrown by loadAllKeys() or loadAll() are
     * not propagated to the caller.
     *
     * @param replaceExistingValues when {@code true}, existing values
     *                              in the Map will be replaced by those
     *                              loaded from the MapLoader
     * @since 3.3
     */
    void loadAll(boolean replaceExistingValues);

    /**
     * Loads the given keys. This is a batch load operation so that an implementation can
     * optimize multiple loads.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * Calls {@link MapLoader#loadAll(java.util.Collection)} on the
     * partitions storing the values with the keys. Exceptions thrown by
     * loadAll() are not propagated to the caller.
     *
     * @param keys                  keys of the values entries to load (keys inside the collection cannot be null)
     * @param replaceExistingValues when {@code true}, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader
     * @since 3.3
     */
    void loadAll(@Nonnull Set<K> keys, boolean replaceExistingValues);

    /**
     * Clears the map and deletes the items from the backing map store.
     * <p>
     * The MAP_CLEARED event is fired for any registered listeners.
     * See {@link com.hazelcast.core.EntryListener#mapCleared(MapEvent)}.
     * <p>
     * To clear the map without removing the items from the map store,
     * use {@link #evictAll}.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * Calls {@link MapStore#deleteAll(Collection)} on each partition
     * with the keys that the given partition stores. Exceptions thrown
     * by deleteAll() are not propagated to the caller.
     *
     * @see #evictAll
     */
    @Override
    void clear();

    /**
     * Asynchronously gets the given key. {@link CompletionStage} can be converted to a
     * {@link java.util.concurrent.CompletableFuture} to obtain the value in a blocking way:
     * <pre>
     *   CompletionStage future = map.getAsync(key);
     *   // do some other stuff, when ready get the result.
     *   Object value = future.toCompletableFuture().get();
     * </pre>
     * Additionally, the client can register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *   // assuming an IMap<String, String>
     *   CompletionStage<String> future = map.getAsync("a");
     *   future.thenAcceptAsync(response -> System.out.println(response));
     * }</pre>
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If value with {@code key} is not found in memory
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by {@code load} fail
     * the operation and are propagated to the caller.
     *
     * @param key the key of the map entry
     * @return CompletionStage from which the value of the key can be retrieved
     * @throws NullPointerException if the specified key is null
     * @see CompletionStage
     */
    CompletionStage<V> getAsync(@Nonnull K key);

    /**
     * Asynchronously puts the given key and value. {@link CompletionStage} can be converted to a
     * {@link java.util.concurrent.CompletableFuture} to obtain the value in a blocking way:
     * <pre>{@code
     *   CompletionStage<Object> future = map.putAsync(key, value);
     *   // do some other stuff, when ready get the result.
     *   Object oldValue = future.toCompletableFuture().get();
     * }</pre>
     * Additionally, the client can register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *   // assuming an IMap<String, String>
     *   CompletionStage<String> future = map.putAsync("a", "b");
     *   future.whenCompleteAsync((v, throwable) -> {
     *     if (throwable == null) {
     *       // do something with the old value returned by put operation
     *     } else {
     *       // handle failure
     *     }
     *   });
     * }</pre>
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p><b>Note:</b>
     * Use {@link #setAsync(Object, Object)} if you don't need the return value, it's slightly more efficient.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @return CompletionStage from which the old value of the key can be retrieved
     * @throws NullPointerException if the specified key or value is null
     * @see CompletionStage
     * @see #setAsync(Object, Object)
     */
    CompletionStage<V> putAsync(@Nonnull K key, @Nonnull V value);

    /**
     * Asynchronously puts the given key and value into this map with a given TTL (time to live) value.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <pre>
     *   CompletionStage future = map.putAsync(key, value, ttl, timeunit);
     *   // do some other stuff, when ready get the result
     *   Object oldValue = future.toCompletableFuture().get();
     * </pre>
     * {@code CompletionStage.toCompletableFuture().get()} will block until the actual map.put() completes.
     * If your application requires a timely response,
     * then you can use Future.get(timeout, timeunit).
     * <pre>
     *   try {
     *     CompletionStage future = map.putAsync(key, newValue, ttl, timeunit);
     *     Object oldValue = future.toCompletableFuture().get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * The client can register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *   // assuming an IMap<String, String>
     *   CompletionStage<String> future = map.putAsync("a", "b", 5, TimeUnit.MINUTES);
     *   future.thenAccept(oldVal -> System.out.println(oldVal));
     * }</pre>
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     * <p><b>Note:</b>
     * Use {@link #setAsync(Object, Object, long, TimeUnit)} if you don't need the return value, it's slightly
     * more efficient.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key     the key of the map entry
     * @param value   the new value of the map entry
     * @param ttl     maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param ttlUnit time unit for the TTL
     * @return CompletionStage from which the old value of the key can be retrieved
     * @throws NullPointerException if the specified key or value is null
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     * @see CompletionStage
     * @see #setAsync(Object, Object, long, TimeUnit)
     */
    CompletionStage<V> putAsync(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit);

    /**
     * Asynchronously puts the given key and value into this map with a given
     * TTL (time to live) value and max idle time value.
     * <p>
     * The entry will expire and get evicted after the TTL. It limits the
     * lifetime of the entries relative to the time of the last write access
     * performed on them. If the TTL is 0, then the entry lives forever.
     * If the TTL is negative, then the TTL from the map configuration will
     * be used (default: forever).
     * <p>
     * The entry will expire and get evicted after the Max Idle time. It limits
     * the lifetime of the entries relative to the time of the last read or write
     * access performed on them. If the MaxIdle is 0, then the entry lives forever.
     * If the MaxIdle is negative, then the MaxIdle from the map configuration will
     * be used (default: forever). The time precision is limited by 1 second.
     * The MaxIdle that is less than 1 second can lead to unexpected behaviour.
     * <pre>
     *   CompletionStage future = map.putAsync(key, value, ttl, timeunit);
     *   // do some other stuff, when ready get the result
     *   Object oldValue = future.toCompletableFuture().get();
     * </pre>
     * {@code CompletionStage.toCompletableFuture().get()} will block until the actual map.put() completes.
     * If your application requires a timely response,
     * then you can use {@code Future.get(timeout, timeunit)}.
     * <pre>
     *   try {
     *     CompletionStage future = map.putAsync(key, newValue, ttl, timeunit);
     *     Object oldValue = future.toCompletableFuture().get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * The client can register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *   // assuming an IMap<String, String>
     *   CompletionStage<String> future = map.putAsync("a", "b", 5, TimeUnit.MINUTES);
     *   future.thenAcceptAsync(oldValue -> System.out.println(oldValue));
     * }</pre>
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     * <p><b>Note:</b>
     * Use {@link #setAsync(Object, Object, long, TimeUnit)} if you don't need
     * the return value, it's slightly more efficient.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key         the key of the map entry
     * @param value       the new value of the map entry
     * @param ttl         maximum time for this entry to stay in the map (0 means infinite, negative
     *                    means map config default)
     * @param ttlUnit     time unit for the TTL
     * @param maxIdle     maximum time for this entry to stay idle in the map.
     *                    (0 means infinite, negative means map config default)
     * @param maxIdleUnit time unit for the Max-Idle
     * @return CompletionStage from which the old value of the key can be retrieved
     * @throws NullPointerException if the specified key, value, ttlUnit or maxIdleUnit are {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     * @see CompletionStage
     * @see #setAsync(Object, Object, long, TimeUnit)
     */
    CompletionStage<V> putAsync(@Nonnull K key, @Nonnull V value,
                                long ttl, @Nonnull TimeUnit ttlUnit,
                                long maxIdle, @Nonnull TimeUnit maxIdleUnit);

    /**
     * Asynchronously copies all of the mappings from the specified map to this map.
     * This version doesn't support batching.
     * <pre>{@code
     *     CompletionStage<Void> future = map.putAllAsync(map);
     *     // do some other stuff, when ready wait for completion
     *     future.toCompletableFuture.get();
     * }</pre>
     * {@code CompletionStage.toCompletableFuture.get()} will block until the actual map.putAll(map) operation completes
     * You can also register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *      CompletionStage<Void> future = map.putAllAsync(map);
     *      future.thenRunAsync(() -> System.out.println("All the entries are added"));
     * }</pre>
     * <p>
     * No atomicity guarantees are given. It could be that in case of failure
     * some of the key/value-pairs get written, while others are not.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * For each element not found in memory
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map, which may come at a significant
     * performance cost. Exceptions thrown by load fail the operation
     * and are propagated to the caller. The elements which were added
     * before the exception was thrown will remain in the map, the rest
     * will not be added.
     * <p>
     * If write-through persistence mode is configured,
     * {@link MapStore#store(Object, Object)} is invoked for each element
     * before the element is added in memory, which may come at a
     * significant performance cost. Exceptions thrown by store fail the
     * operation and are propagated to the caller. The elements which
     * were added before the exception was thrown will remain in the map,
     * the rest will not be added.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     * @param map mappings to be stored in this map
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon putAll operation completion
     * @see CompletionStage
     *
     * @since 4.1
     */
    CompletionStage<Void> putAllAsync(@Nonnull Map<? extends K, ? extends V> map);

    /**
     * Asynchronously puts the given key and value.
     * The entry lives forever.
     * Similar to the put operation except that set
     * doesn't return the old value, which is more efficient.
     * <pre>{@code
     *   CompletionStage<Void> future = map.setAsync(key, value);
     *   // do some other stuff, when ready wait for completion
     *   future.toCompletableFuture().get();
     * }</pre>
     * {@code CompletionStage.toCompletableFuture().get()} will block until the actual map.set() operation completes.
     * You can also register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *   CompletionStage<Void> future = map.setAsync("a", "b");
     *   future.thenRunAsync(() -> System.out.println("Value is now set to b."));
     * }</pre>
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon set operation completion
     * @throws NullPointerException if the specified key or value is null
     * @see CompletionStage
     */
    CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull V value);

    /**
     * Asynchronously puts an entry into this map with a given TTL (time to live) value,
     * without returning the old value (which is more efficient than {@code put()}).
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <pre>
     *   CompletionStage&lt;Void&gt; future = map.setAsync(key, value, ttl, timeunit);
     *   // do some other stuff, when you want to make sure set operation is complete:
     *   future.toCompletableFuture().get();
     * </pre>
     * {@code CompletionStage.toCompletableFuture().get()} will block until the actual map set operation completes.
     * If your application requires a timely response,
     * then you can use {@code CompletionStage.toCompletableFuture().get(long, TimeUnit)}.
     * <pre>
     *   try {
     *     CompletionStage&lt;Void&gt; future = map.setAsync(key, newValue, ttl, timeunit);
     *     future.toCompletableFuture().get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * You can also register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>
     *   CompletionStage&lt;Void&gt; future = map.setAsync("a", "b", 5, TimeUnit.MINUTES);
     *   future.thenRunAsync(() -&gt; System.out.println("done"));
     * </pre>
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key     the key of the map entry
     * @param value   the new value of the map entry
     * @param ttl     maximum time for this entry to stay in the map (0 means infinite, negative
     *                means map config default)
     * @param ttlUnit time unit for the TTL
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon set operation completion
     * @throws NullPointerException if the specified key, value, ttlUnit
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     * @see CompletionStage
     */
    CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit);

    /**
     * Asynchronously puts an entry into this map with a given TTL (time to live)
     * value and max idle time value without returning the old value
     * (which is more efficient than {@code put()}).
     * <p>
     * The entry will expire and get evicted after the TTL. It limits the
     * lifetime of the entries relative to the time of the last write access
     * performed on them. If the TTL is 0, then the entry lives forever.
     * If the TTL is negative, then the TTL from the map configuration will
     * be used (default: forever).
     * <p>
     * The entry will expire and get evicted after the Max Idle time. It limits
     * the lifetime of the entries relative to the time of the last read or write
     * access performed on them. If the MaxIdle is 0, then the entry lives forever.
     * If the MaxIdle is negative, then the MaxIdle from the map configuration will
     * be used (default: forever). The time precision is limited by 1 second.
     * The MaxIdle that less than 1 second can lead to unexpected behaviour.
     * <pre>
     *   CompletionStage&lt;Void&gt; future = map.setAsync(key, value, ttl, timeunit);
     *   // do some other stuff, when you want to make sure set operation is complete:
     *   future.toCompletableFuture().get();
     * </pre>
     * {@code CompletionStage.toCompletableFuture().get()} will block until the actual map set operation
     * completes. If your application requires a timely response,
     * then you can use {@code CompletionStage.toCompletableFuture().get(long, TimeUnit)}.
     * <pre>
     *   try {
     *     CompletionStage&lt;Void&gt; future = map.setAsync(key, newValue, ttl, timeunit);
     *     future.toCompletableFuture().get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * You can also register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>
     *   CompletionStage&lt;Void&gt; future = map.setAsync("a", "b", 5, TimeUnit.MINUTES);
     *   future.thenRunAsync(() -&gt; System.out.println("Done"));
     * </pre>
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to
     * the next closest second value.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key         the key of the map entry
     * @param value       the new value of the map entry
     * @param ttl         maximum time for this entry to stay in the map (0 means infinite, negative
     *                    means map config default)
     * @param ttlUnit     time unit for the TTL
     * @param maxIdle     maximum time for this entry to stay idle in the map.
     *                    (0 means infinite, negative means map config default)
     * @param maxIdleUnit time unit for the Max-Idle
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon set operation completion
     * @throws NullPointerException if the specified key, value, ttlUnit or maxIdleUnit are {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     * @see CompletionStage
     */
    CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull V value,
                                   long ttl, @Nonnull TimeUnit ttlUnit,
                                   long maxIdle, @Nonnull TimeUnit maxIdleUnit);

    /**
     * Asynchronously removes the given key, returning an {@link CompletionStage}
     * on which the caller can register further computation stages to be invoked
     * upon remove operation completion or block waiting for the operation to
     * complete using one of blocking ways to wait on
     * {@link CompletionStage#toCompletableFuture()}.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is removed from the memory, {@link MapStore#delete(Object)}
     * is called to remove the value from the map store. Exceptions
     * thrown by delete fail the operation and are propagated to the
     * caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key The key of the map entry to remove
     * @return {@link CompletionStage} from which the value removed from the map can be retrieved
     * @throws NullPointerException if the specified key is {@code null}
     * @see CompletionStage
     */
    CompletionStage<V> removeAsync(@Nonnull K key);

    /**
     * Asynchronously removes the given key, returning an {@link CompletionStage}
     * on which the caller can register further computation stages to be invoked
     * upon delete operation completion or block waiting for the operation to
     * complete using one of blocking ways to wait on
     * {@link CompletionStage#toCompletableFuture()}.
     *
     * <p>
     * Unlike {@link #removeAsync(Object)}, this operation does not return
     * the removed value, which avoids the serialization and network transfer cost of the
     * returned value. If the removed value will not be used, this operation
     * is preferred over the removeAsync operation for better performance.
     *
     * <p>The returned {@link CompletionStage} completes with a boolean value:
     * {@code true} if the key is in memory and deletion is successful,
     * {@code false} otherwise.
     *
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is removed from the memory, {@link MapStore#delete(Object)}
     * is called to remove the value from the map store. Exceptions
     * thrown by delete fail the operation and are propagated to the
     * caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key The key of the map entry to remove
     * @return {@link CompletionStage} which completes with a {@code boolean} value, indicating the result of the deletion
     * @throws NullPointerException if the specified key is {@code null}
     * @see CompletionStage
     */
    CompletionStage<Boolean> deleteAsync(@Nonnull K key);

    /**
     * Tries to remove the entry with the given key from this map
     * within the specified timeout value. If the key is already locked by another
     * thread and/or member, then this operation will wait the timeout
     * amount for acquiring the lock.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is removed from the memory, {@link MapStore#delete(Object)}
     * is called to remove the value from the map store. Exceptions
     * thrown by delete fail the operation and are propagated to the
     * caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key      key of the entry
     * @param timeout  maximum time to wait for acquiring the lock for the key
     * @param timeunit time unit for the timeout
     * @return {@code true} if the remove is successful, {@code false} otherwise
     * @throws NullPointerException if the specified key is {@code null}
     */
    boolean tryRemove(@Nonnull K key, long timeout, @Nonnull TimeUnit timeunit);

    /**
     * Tries to put the given key and value into this map within a specified
     * timeout value. If this method returns false, it means that
     * the caller thread could not acquire the lock for the key within the
     * timeout duration, thus the put operation is not successful.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param timeout  maximum time to wait
     * @param timeunit time unit for the timeout
     * @return {@code true} if the put is successful, {@code false} otherwise
     * @throws NullPointerException if the specified key or value is {@code null}
     */
    boolean tryPut(@Nonnull K key, @Nonnull V value,
                   long timeout, @Nonnull TimeUnit timeunit);

    /**
     * Puts an entry into this map with a given TTL (time to live) value.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the
     * next closest second value.
     * <p><b>Note:</b>
     * Use {@link #set(Object, Object, long, TimeUnit)} if you don't need the
     * return value, it's slightly more efficient.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key     key of the entry
     * @param value   value of the entry
     * @param ttl     maximum time for this entry to stay in the map (0 means infinite, negative
     *                means map config default)
     * @param ttlUnit time unit for the TTL
     * @return old value of the entry
     * @throws NullPointerException if the specified key or value is {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    @Override
    V put(@Nonnull K key, @Nonnull V value,
          long ttl, @Nonnull TimeUnit ttlUnit);

    /**
     * Puts an entry into this map with a given TTL (time to live) value and
     * max idle time value.
     * <p>
     * The entry will expire and get evicted after the TTL. It limits the
     * lifetime of the entries relative to the time of the last write access
     * performed on them. If the TTL is 0, then the entry lives forever.
     * If the TTL is negative, then the TTL from the map configuration will
     * be used (default: forever).
     * <p>
     * The entry will expire and get evicted after the Max Idle time. It limits
     * the lifetime of the entries relative to the time of the last read or write
     * access performed on them. If the MaxIdle is 0, then the entry lives forever.
     * If the MaxIdle is negative, then the MaxIdle from the map configuration will
     * be used (default: forever). The time precision is limited by 1 second.
     * The MaxIdle that less than 1 second can lead to unexpected behaviour.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     * <p><b>Note:</b>
     * Use {@link #set(Object, Object, long, TimeUnit)} if you don't need the
     * return value, it's slightly more efficient.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key         key of the entry
     * @param value       value of the entry
     * @param ttl         maximum time for this entry to stay in the map (0 means infinite, negative
     *                    means map config default)
     * @param ttlUnit     time unit for the TTL
     * @param maxIdle     maximum time for this entry to stay idle in the map.
     *                    (0 means infinite, negative means map config default)
     * @param maxIdleUnit time unit for the Max-Idle
     * @return old value of the entry
     * @throws NullPointerException if the specified key, value, ttlUnit or maxIdleUnit are {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    V put(@Nonnull K key, @Nonnull V value,
          long ttl, @Nonnull TimeUnit ttlUnit,
          long maxIdle, @Nonnull TimeUnit maxIdleUnit);

    /**
     * Same as {@link #put(Object, Object, long, TimeUnit)}
     * except that the map store, if defined, will not be called to
     * load/store/persist the entry.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to
     * next closest second value.
     *
     * @param key     key of the entry
     * @param value   value of the entry
     * @param ttl     maximum time for this entry to stay in the map (0 means infinite, negative
     *                means map config default)
     * @param ttlUnit time unit for the TTL
     * @throws NullPointerException if the specified key or value is {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    void putTransient(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit);

    /**
     * Same as {@link #put(Object, Object, long, TimeUnit)} except that the map
     * store, if defined, will not be called to load/store/persist the entry.
     * <p>
     * The entry will expire and get evicted after the TTL. It limits the
     * lifetime of the entries relative to the time of the last write access
     * performed on them. If the TTL is 0, then the entry lives forever.
     * If the TTL is negative, then the TTL from the map configuration will
     * be used (default: forever).
     * <p>
     * The entry will expire and get evicted after the Max Idle time. It limits
     * the lifetime of the entries relative to the time of the last read or write
     * access performed on them. If the MaxIdle is 0, then the entry lives forever.
     * If the MaxIdle is negative, then the MaxIdle from the map configuration will
     * be used (default: forever). The time precision is limited by 1 second.
     * The MaxIdle that less than 1 second can lead to unexpected behaviour.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to
     * next closest second value.
     *
     * @param key         key of the entry
     * @param value       value of the entry
     * @param ttl         maximum time for this entry to stay in the map (0 means infinite, negative
     *                    means map config default)
     * @param ttlUnit     time unit for the TTL
     * @param maxIdle     maximum time for this entry to stay idle in the map.
     *                    (0 means infinite, negative means map config default)
     * @param maxIdleUnit time unit for the Max-Idle
     * @throws NullPointerException if the specified {@code key}, {@code value}, {@code ttlUnit} or
     *                              {@code maxIdleUnit} are {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    void putTransient(@Nonnull K key, @Nonnull V value,
                      long ttl, @Nonnull TimeUnit ttlUnit,
                      long maxIdle, @Nonnull TimeUnit maxIdleUnit);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Note:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * Also, this method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @return a clone of the previous value
     * @throws NullPointerException if the specified {@code key} or {@code value}
     *                              is {@code null}
     */
    @Override
    V putIfAbsent(@Nonnull K key, @Nonnull V value);

    /**
     * Puts an entry into this map with a given TTL (time to live) value,
     * if the specified key is not already associated with a value.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to
     * the next closest second value.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key     key of the entry
     * @param value   value of the entry
     * @param ttl     maximum time for this entry to stay in the map (0 means infinite, negative
     *                means map config default)
     * @param ttlUnit time unit for the TTL
     * @return old value of the entry
     * @throws NullPointerException if the specified {@code key} or {@code value}
     *                              is {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    V putIfAbsent(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit);

    /**
     * Puts an entry into this map with a given TTL (time to live) value and
     * max idle time value.
     * if the specified key is not already associated with a value.
     * <p>
     * The entry will expire and get evicted after the TTL. It limits the
     * lifetime of the entries relative to the time of the last write access
     * performed on them. If the TTL is 0, then the entry lives forever.
     * If the TTL is negative, then the TTL from the map configuration will
     * be used (default: forever).
     * <p>
     * The entry will expire and get evicted after the Max Idle time. It limits
     * the lifetime of the entries relative to the time of the last read or write
     * access performed on them. If the MaxIdle is 0, then the entry lives forever.
     * If the MaxIdle is negative, then the MaxIdle from the map configuration will
     * be used (default: forever). The time precision is limited by 1 second.
     * The MaxIdle that less than 1 second can lead to unexpected behaviour.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the
     * next closest second value.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If no value is found with {@code key} in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map. Exceptions thrown by load fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key         key of the entry
     * @param value       value of the entry
     * @param ttl         maximum time for this entry to stay in the map (0 means infinite, negative
     *                    means map config default)
     * @param ttlUnit     time unit for the TTL
     * @param maxIdle     maximum time for this entry to stay idle in the map.
     *                    (0 means infinite, negative means map config default)
     * @param maxIdleUnit time unit for the Max-Idle
     * @return old value of the entry
     * @throws NullPointerException if the specified {@code key}, {@code value}, {@code ttlUnit} or
     *                              {@code maxIdleUnit} are {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    V putIfAbsent(@Nonnull K key, @Nonnull V value,
                  long ttl, @Nonnull TimeUnit ttlUnit,
                  long maxIdle, @Nonnull TimeUnit maxIdleUnit);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method may return {@code false} even if the operation succeeds.<br>
     * Background: If the partition owner for given key goes down after successful
     * value replace, but before the executing node retrieved the invocation
     * result response, then the operation is retried. The invocation retry fails
     * because the value is already updated and the result of such replace call
     * returns {@code false}. Hazelcast doesn't guarantee exactly once invocation.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If value with {@code key} is not found in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from the map
     * store backing the map.
     * <p>
     * If write-through persistence mode is configured, before the value is
     * stored in memory, {@link MapStore#store(Object, Object)} is called to
     * write the value into the map store. Exceptions thrown by the store fail
     * the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with write-coalescing
     * turned off, {@link com.hazelcast.map.ReachedMaxSizeException} may be
     * thrown if the write-behind queue has reached its per-node maximum capacity.
     *
     * @throws NullPointerException if any of the specified parameters are {@code null}
     */
    @Override
    boolean replace(@Nonnull K key, @Nonnull V oldValue, @Nonnull V newValue);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If value with {@code key} is not found in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map.
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @throws NullPointerException if the specified key or value is {@code null}
     */
    @Override
    V replace(@Nonnull K key, @Nonnull V value);

    /**
     * Puts an entry into this map without returning the old value
     * (which is more efficient than {@code put()}).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method breaks the contract of EntryListener.
     * When an entry is updated by set(), it fires an EntryEvent with a null oldValue.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key   key of the entry
     * @param value value of the entry
     * @throws NullPointerException if the specified key or value is {@code null}
     */
    @Override
    void set(@Nonnull K key, @Nonnull V value);

    /**
     * Puts an entry into this map with a given TTL (time to live) value,
     * without returning the old value (which is more efficient than {@code put()}).
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the
     * next closest second value.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key     key of the entry
     * @param value   value of the entry
     * @param ttl     maximum time for this entry to stay in the map (0 means infinite, negative
     *                means map config default)
     * @param ttlUnit time unit for the TTL
     * @throws NullPointerException if the specified key or value is {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    void set(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit);

    /**
     * Puts an entry into this map with a given TTL (time to live) value and
     * max idle time value without returning the old value (which is more
     * efficient than {@code put()}).
     * <p>
     * The entry will expire and get evicted after the TTL. It limits the
     * lifetime of the entries relative to the time of the last write access
     * performed on them. If the TTL is 0, then the entry lives forever.
     * If the TTL is negative, then the TTL from the map configuration will
     * be used (default: forever).
     * <p>
     * The entry will expire and get evicted after the Max Idle time. It limits
     * the lifetime of the entries relative to the time of the last read or write
     * access performed on them. If the MaxIdle is 0, then the entry lives forever.
     * If the MaxIdle is negative, then the MaxIdle from the map configuration will
     * be used (default: forever). The time precision is limited by 1 second.
     * The MaxIdle that less than 1 second can lead to unexpected behaviour.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form
     * of the {@code key}, not the actual implementations of {@code hashCode}
     * and {@code equals} defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to
     * the next closest second value.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured, before the value
     * is stored in memory, {@link MapStore#store(Object, Object)} is
     * called to write the value into the map store. Exceptions thrown
     * by the store fail the operation and are propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key         key of the entry
     * @param value       value of the entry
     * @param ttl         maximum time for this entry to stay in the map (0 means infinite, negative
     *                    means map config default)
     * @param ttlUnit     time unit for the TTL
     * @param maxIdle     maximum time for this entry to stay idle in the map.
     *                    (0 means infinite, negative means map config default)
     * @param maxIdleUnit time unit for the Max-Idle
     * @throws NullPointerException if the specified key, value, ttlUnit or maxIdleUnit are {@code null}
     * @throws UnsupportedOperationException if the underlying map storage doesn't
     *         support TTL-based expiration (all in-memory storages support it).
     */
    void set(@Nonnull K key, @Nonnull V value,
             long ttl, @Nonnull TimeUnit ttlUnit,
             long maxIdle, @Nonnull TimeUnit maxIdleUnit);

    /**
     * Copies all of the mappings from the specified map to this map without loading
     * non-existing elements from map store (which is more efficient than {@code putAll()}).
     * <p>
     * This method breaks the contract of EntryListener.
     * EntryEvent of all the updated entries will have null oldValue even if they exist previously.
     * <p>
     * No atomicity guarantees are given. It could be that in case of failure
     * some of the key/value-pairs get written, while others are not.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured,
     * {@link MapStore#store(Object, Object)} is invoked for each element
     * before the element is added in memory, which may come at a
     * significant performance cost. Exceptions thrown by store fail the
     * operation and are propagated to the caller. The elements which
     * were added before the exception was thrown will remain in the map,
     * the rest will not be added.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @since 4.1
     */
    void setAll(@Nonnull Map<? extends K, ? extends V> map);

    /**
     * Asynchronously copies all of the mappings from the specified map to this map
     * without loading non-existing elements from map store. This version doesn't
     * support batching.
     * <pre>{@code
     *     CompletionStage<Void> future = map.setAllAsync(map);
     *     // do some other stuff, when ready wait for completion
     *     future.toCompletableFuture.get();
     * }</pre>
     * {@code CompletionStage.toCompletableFuture.get()} will block until the actual map.setAll(map) operation completes
     * You can also register further computation stages to be invoked upon
     * completion of the {@code CompletionStage} via any of {@link CompletionStage}
     * methods:
     * <pre>{@code
     *      CompletionStage<Void> future = map.setAllAsync(map);
     *      future.thenRunAsync(() -> System.out.println("All the entries are set"));
     * }</pre>
     * <p>
     * This method breaks the contract of EntryListener.
     * EntryEvent of all the updated entries will have null oldValue even if they exist previously.
     * <p>
     * No atomicity guarantees are given. It could be that in case of failure
     * some of the key/value-pairs get written, while others are not.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If write-through persistence mode is configured,
     * {@link MapStore#store(Object, Object)} is invoked for each element
     * before the element is added in memory, which may come at a
     * significant performance cost. Exceptions thrown by store fail the
     * operation and are propagated to the caller. The elements which
     * were added before the exception was thrown will remain in the map,
     * the rest will not be added.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     * @param map mappings to be stored in this map
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon setAll operation completion
     * @see CompletionStage
     *
     * @since 4.1
     */
    CompletionStage<Void> setAllAsync(@Nonnull Map<? extends K, ? extends V> map);

    /**
     * Acquires the lock for the specified key.
     * <p>
     * If the lock is not available, then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * <p>
     * You get a lock whether the value is present in the map or not. Other
     * threads (possibly on other systems) would block on their invoke of
     * {@code lock()} until the non-existent key is unlocked. If the lock
     * holder introduces the key to the map, the {@code put()} operation
     * is not blocked. If a thread not holding a lock on the non-existent key
     * tries to introduce the key while a lock exists on the non-existent key,
     * the {@code put()} operation blocks until it is unlocked.
     * <p>
     * Scope of the lock is this map only.
     * Acquired lock is only for the key in this map.
     * <p>
     * Locks are re-entrant so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     * <p>
     * There is no lock timeout on this method. Locks will be held infinitely.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key key to lock
     * @throws NullPointerException if the specified key is {@code null}
     */
    void lock(@Nonnull K key);

    /**
     * Acquires the lock for the specified key for the specified lease time.
     * <p>
     * After lease time, the lock will be released.
     * <p>
     * If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p>
     * Scope of the lock is this map only.
     * Acquired lock is only for the key in this map.
     * <p>
     * Locks are re-entrant, so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key       the key to lock
     * @param leaseTime time to wait before releasing the lock
     * @param timeUnit  unit of time to specify lease time
     * @throws NullPointerException     if the specified key is {@code null}
     * @throws IllegalArgumentException if the leaseTime is not positive
     */
    void lock(@Nonnull K key, long leaseTime, @Nullable TimeUnit timeUnit);

    /**
     * Checks the lock for the specified key.
     * <p>
     * If the lock is acquired then returns true, else returns false.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the key that is checked for lock
     * @return {@code true} if lock is acquired, {@code false} otherwise
     * @throws NullPointerException if the specified key is {@code null}
     */
    boolean isLocked(@Nonnull K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>
     * If the lock is not available then the current thread
     * doesn't wait and returns false immediately.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the key to lock
     * @return {@code true} if lock is acquired, {@code false} otherwise
     * @throws NullPointerException if the specified key is {@code null}
     */
    boolean tryLock(@Nonnull K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>
     * If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>the lock is acquired by the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key      key to lock in this map
     * @param time     maximum time to wait for the lock
     * @param timeunit time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired, {@code false} if the waiting time
     * elapsed before the lock was acquired
     * @throws NullPointerException if the specified key is {@code null}
     * @throws InterruptedException if interrupted while trying to acquire the lock
     */
    boolean tryLock(@Nonnull K key, long time, @Nullable TimeUnit timeunit) throws InterruptedException;

    /**
     * Tries to acquire the lock for the specified key for the specified lease time.
     * <p>
     * After lease time, the lock will be released.
     * <p>
     * If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>the lock is acquired by the current thread, or
     * <li>the specified waiting time elapses.
     * </ul>
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key           key to lock in this map
     * @param time          maximum time to wait for the lock
     * @param timeunit      time unit of the {@code time} argument
     * @param leaseTime     time to wait before releasing the lock
     * @param leaseTimeunit unit of time to specify lease time
     * @return {@code true} if the lock was acquired, {@code false} if the waiting time
     * elapsed before the lock was acquired
     * @throws NullPointerException if the specified key is {@code null}
     * @throws InterruptedException if interrupted while trying to acquire the lock
     */
    boolean tryLock(@Nonnull K key,
                    long time, @Nullable TimeUnit timeunit,
                    long leaseTime, @Nullable TimeUnit leaseTimeunit)
            throws InterruptedException;

    /**
     * Releases the lock for the specified key. It never blocks and
     * returns immediately.
     * <p>
     * If the current thread is the holder of this lock, then the hold
     * count is decremented.  If the hold count is zero, then the lock
     * is released.  If the current thread is not the holder of this
     * lock, then {@link IllegalMonitorStateException} is thrown.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the key to unlock
     * @throws NullPointerException         if the specified key is {@code null}
     * @throws IllegalMonitorStateException if the current thread does not hold this lock
     */
    void unlock(@Nonnull K key);

    /**
     * Releases the lock for the specified key regardless of the lock owner.
     * It always successfully unlocks the key, never blocks,
     * and returns immediately.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the key to unlock
     * @throws NullPointerException if the specified key is {@code null}
     */
    void forceUnlock(@Nonnull K key);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     * <p>
     * Note that entries in distributed map are partitioned across
     * the cluster members; each member owns and manages some portion of the
     * entries. Owned entries are called local entries. This
     * listener will be listening for the events of local entries. Let's say
     * your cluster has member1 and member2. On member2 you added a local listener and from
     * member1, you call {@code map.put(key2, value2)}.
     * If the key2 is owned by member2 then the local listener will be
     * notified for the add/update event. Also note that entries can migrate to
     * other nodes for load balancing and/or membership change.
     *
     * @param listener {@link MapListener} for this map
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws UnsupportedOperationException if this operation is not supported, for example on a Hazelcast client
     * @throws NullPointerException          if the listener is {@code null}
     * @see #localKeySet()
     * @see MapListener
     */
    UUID addLocalEntryListener(@Nonnull MapListener listener);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener}
     * sub-interface for that event.
     * The listener will get notified for map events filtered by the given predicate.
     *
     * @param listener     {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws UnsupportedOperationException if this operation isn't supported, for example on a Hazelcast client
     * @throws NullPointerException          if the {@code listener} or {@code predicate} is {@code null}
     * @throws IllegalArgumentException      if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                       {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                       {@link com.hazelcast.query.PagingPredicate}
     * @see MapListener
     */
    UUID addLocalEntryListener(@Nonnull MapListener listener,
                               @Nonnull Predicate<K, V> predicate,
                               boolean includeValue);

    /**
     * Adds a local entry listener for this map.
     * <p>
     * The added listener will only be listening for the events
     * (add/remove/update/evict) of the locally owned entries.
     * The listener will get notified for map add/remove/update/evict events
     * filtered by the given predicate.
     *
     * @param listener     {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param key          key to listen for
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException          if the listener is {@code null}
     * @throws NullPointerException          if the predicate is {@code null}
     * @throws UnsupportedOperationException if this operation isn't supported, for example on a Hazelcast client
     * @throws IllegalArgumentException      if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                       {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                       {@link com.hazelcast.query.PagingPredicate}
     * @see MapListener
     */
    UUID addLocalEntryListener(@Nonnull MapListener listener,
                               @Nonnull Predicate<K, V> predicate,
                               @Nullable K key,
                               boolean includeValue);

    /**
     * Adds an interceptor for this map.
     * <p>
     * Added interceptor will intercept operations and execute user defined methods.
     * They will cancel operations if the user defined method throws an exception.
     *
     * @param interceptor map interceptor
     * @return ID of registered interceptor
     */
    String addInterceptor(@Nonnull MapInterceptor interceptor);

    /**
     * Removes the given interceptor for this map,
     * so it will not intercept operations anymore.
     *
     * @param id registration ID of the map interceptor
     * @return {@code true} if registration is removed, {@code false} otherwise
     */
    boolean removeInterceptor(@Nonnull String id);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener}
     * sub-interface for that event.
     *
     * @param listener     {@link MapListener} for this map
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is {@code null}
     * @see MapListener
     */
    UUID addEntryListener(@Nonnull MapListener listener, boolean includeValue);

    /**
     * Removes the specified entry listener.
     * <p>
     * Returns silently if there is no such listener added before.
     *
     * @param id ID of registered listener
     * @return true if registration is removed, false otherwise
     */
    boolean removeEntryListener(@Nonnull UUID id);

    /**
     * Adds a MapPartitionLostListener.
     * <p>
     * The method returns a register ID. This ID is needed to remove the
     * {@link MapPartitionLostListener} using the
     * {@link #removePartitionLostListener(UUID)} method.
     * <p>
     * There is no check for duplicate registrations, so if you register the
     * listener twice, you will receive events twice.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * Please see {@link com.hazelcast.partition.PartitionLostListener} for weaknesses.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Listeners registered from HazelcastClient may miss some of the map
     * partition lost events due to design limitations.
     *
     * @param listener the added MapPartitionLostListener
     * @return returns the registration ID for the MapPartitionLostListener
     * @throws java.lang.NullPointerException if listener is {@code null}
     * @see #removePartitionLostListener(UUID)
     * @see com.hazelcast.partition.PartitionLostListener
     */
    UUID addPartitionLostListener(@Nonnull MapPartitionLostListener listener);

    /**
     * Removes the specified map partition lost listener.
     * <p>
     * Returns silently if there is no such listener was added before.
     *
     * @param id ID of registered listener
     * @return true if registration is removed, false otherwise
     * @throws NullPointerException if {@code id} is {@code null}
     */
    boolean removePartitionLostListener(@Nonnull UUID id);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param listener     {@link MapListener} for this map
     * @param key          key to listen for
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is {@code null}
     * @throws NullPointerException if the specified key is {@code null}
     * @see MapListener
     */
    UUID addEntryListener(@Nonnull MapListener listener, @Nonnull K key, boolean includeValue);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener}
     * sub-interface for that event.
     *
     * @param listener     the added continuous {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException     if the specified {@code listener} or {@code predicate}
     *                                  is {@code null}
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @see MapListener
     */
    UUID addEntryListener(@Nonnull MapListener listener,
                          @Nonnull Predicate<K, V> predicate,
                          boolean includeValue);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener} sub-interface for that event.
     *
     * @param listener     the continuous {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param key          key to listen for
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException     if the specified {@code listener} or {@code predicate} is {@code null}
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @see MapListener
     */
    UUID addEntryListener(@Nonnull MapListener listener,
                          @Nonnull Predicate<K, V> predicate,
                          @Nullable K key,
                          boolean includeValue);

    /**
     * Returns the {@code EntryView} for the specified key.
     * <p>
     * <b>Not to misuse this method, please know these points:</b>
     * <lu>
     *  <li>This method cannot be used as a replacement for {@link IMap#get}.</li>
     *  <li>This method only looks up entries already in memory and
     *  does not load missing ones from {@link MapStore}.</li>
     *  <li>Calling this method does not update entry or map level statistics.</li>
     *  <li>Calling this method is not counted as an access,
     *  so it doesn't have any effect on eviction and expiration.</li>
     * </lu>
     * <p>
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method returns a clone of original mapping, modifying the returned value does not change
     * the actual value in the map. One should put modified value back to make changes visible to all nodes.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the key of the entry
     * @return {@code EntryView} of the specified key
     * @throws NullPointerException if the specified key is {@code null}
     * @see EntryView
     */
    EntryView<K, V> getEntryView(@Nonnull K key);

    /**
     * Evicts the specified key from this map.
     * <p>
     * If a {@code MapStore} is defined for this map, then the entry is
     * not deleted from the underlying {@code MapStore}, evict only
     * removes the entry from the memory. Use {@link #delete(Object)} or
     * {@link #remove(Object)} if {@link MapStore#delete(Object)} needs
     * to be called.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the
     * binary form of the {@code key}, not the actual implementations of
     * {@code hashCode} and {@code equals} defined in the {@code key}'s
     * class.
     *
     * @param key the specified key to evict from this map
     * @return {@code true} if the key is evicted, {@code false} otherwise
     * @throws NullPointerException if the specified key is {@code null}
     * @see #delete(Object)
     * @see #remove(Object)
     */
    boolean evict(@Nonnull K key);

    /**
     * Evicts all keys from this map except the locked ones.
     * <p>
     * If a {@code MapStore} is defined for this map, deleteAll is
     * <strong>not</strong> called by this method. If you do want to
     * {@link MapStore#deleteAll(Collection)} to be called use the
     * {@link #clear()} method.
     * <p>
     * The EVICT_ALL event is fired for any registered listeners.
     * See {@link com.hazelcast.core.EntryListener#mapEvicted(MapEvent)} .
     *
     * @see #clear()
     * @since 3.3
     */
    void evictAll();

    /**
     * Returns an immutable set clone of the keys contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set.
     * <p>
     * This method performs a distributed query to collect all keys, which may
     * throw a {@link QueryResultSizeExceededException} if the
     * {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured and exceeded.
     * <b>Warning:</b>
     * <p>
     * Using this method is not recommended unless the entire set of keys
     * can fit in a single member's memory. Fetching the entire distributed collection
     * into one machine can lead to significant memory issues and is typically discouraged
     * in distributed system design. If you must use this method, it is advisable to configure
     * the {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} property to avoid potential
     * {@link OutOfMemoryError}.
     *
     * @return an immutable set clone of the keys contained in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Nonnull
    @Override
    Set<K> keySet();

    /**
     * Returns an immutable collection clone of the values contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection.
     * <p>
     * This method performs a distributed query to collect all values, which may
     * throw a {@link QueryResultSizeExceededException} if the
     * {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured and exceeded.
     * <p>
     * <b>Warning:</b>
     * <p>
     * Using this method is not recommended unless the entire set of values
     * can fit in a single member's memory. Fetching the entire distributed collection
     * into one machine can lead to significant memory issues and is typically discouraged
     * in distributed system design. If you must use this method, it is advisable to configure
     * the {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} property to avoid potential
     * {@link OutOfMemoryError}.
     *
     * @return an immutable collection clone of the values contained in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Nonnull
    @Override
    Collection<V> values();

    /**
     * Returns an immutable {@link Set} clone of the mappings contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set.
     * <p>
     * This method performs a distributed query to collect all entries, which may
     * throw a {@link QueryResultSizeExceededException} if the
     * {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured and exceeded.
     * <p>
     * <b>Warning:</b>
     * <p>
     * Using this method is not recommended unless the entire set of entries
     * can fit in a single member's memory. Fetching the entire distributed collection
     * into one machine can lead to significant memory issues and is typically discouraged
     * in distributed system design. If you must use this method, it is advisable to configure
     * the {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} property to avoid potential
     * {@link OutOfMemoryError}.
     *
     * @return an immutable set clone of the keys mappings in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Nonnull
    @Override
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Queries the map based on the specified predicate and
     * returns an immutable {@link Set} clone of the keys of matching entries.
     * <p>
     * Specified predicate runs on all members in parallel.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return result key set of the query
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws NullPointerException             if the predicate is {@code null}
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Override
    Set<K> keySet(@Nonnull Predicate<K, V> predicate);

    /**
     * Queries the map based on the specified predicate and returns an immutable set of the matching entries.
     * <p>
     * Specified predicate runs on all members in parallel.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return result entry set of the query
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws NullPointerException             if the predicate is {@code null}
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<Map.Entry<K, V>> entrySet(@Nonnull Predicate<K, V> predicate);

    /**
     * Queries the map based on the specified predicate and returns an immutable
     * collection of the values of matching entries.
     * <p>
     * Specified predicate runs on all members in parallel.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return result value collection of the query
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws NullPointerException             if the predicate is {@code null}
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Override
    Collection<V> values(@Nonnull Predicate<K, V> predicate);

    /**
     * Returns an immutable collection locally owned values contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return an immutable collection clone of the values contained in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     * @since 5.4.0
     */
    Collection<V> localValues();

   /**
    * Queries the map of locally owned keys based on the specified predicate and returns an immutable
    * collection of the values of matching entries.
    * <p>
    * Specified predicate runs on local member.
    * <p>
    * <b>Warning:</b>
    * <p>
    * The collection is <b>NOT</b> backed by the map,
    * so changes to the map are <b>NOT</b> reflected in the collection.
    * <p>
    * This method is always executed by a distributed query,
    * so it may throw a {@link QueryResultSizeExceededException}
    * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
    *
    * @param predicate specified query criteria
    * @return result value collection of the query
    * @throws QueryResultSizeExceededException if query result size limit is exceeded
    * @throws NullPointerException             if the predicate is {@code null}
    * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
    * @since 5.4.0
    */
    Collection<V> localValues(@Nonnull Predicate<K, V> predicate);

    /**
     * Returns the locally owned immutable set of keys.
     * <p>
     * Each key in this map is owned and managed by a specific
     * member in the cluster.
     * <p>
     * Note that ownership of these keys might change over time
     * so that key ownerships can be almost evenly distributed
     * in the cluster.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return locally owned immutable set of keys
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> localKeySet();

    /**
     * Returns an immutable set of the keys of matching locally owned entries.
     * <p>
     * Each key in this map is owned and managed by a specific
     * member in the cluster.
     * <p>
     * Note that ownership of these keys might change over time
     * so that key ownerships can be almost evenly distributed
     * in the cluster.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link ClusterProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return an immutable set of the keys of matching locally owned entries
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws UnsupportedOperationException    if this operation isn't supported, for example on a Hazelcast client
     * @see ClusterProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> localKeySet(@Nonnull Predicate<K, V> predicate);

    /**
     * Convenient method to add an index to this map with the given type and attributes.
     * Attributes are indexed in ascending order.
     * <p>
     *
     * @param type       Index type.
     * @param attributes Attributes to be indexed.
     * @see #addIndex(IndexConfig)
     */
    default void addIndex(IndexType type, String... attributes) {
        IndexConfig config = IndexUtils.createIndexConfig(type, attributes);

        addIndex(config);
    }

    /**
     * Adds an index to this map for the specified entries so
     * that queries can run faster.
     * <p>
     * Let's say your map values are Employee objects.
     * <pre>
     *   public class Employee implements Serializable {
     *     private boolean active = false;
     *     private int age;
     *     private String name = null;
     *     // other fields
     *
     *     // getters setter
     *   }
     * </pre>
     * If you are querying your values mostly based on age and active then
     * you may consider indexing these fields.
     * <pre>
     *   IMap imap = Hazelcast.getMap("employees");
     *   imap.addIndex(new IndexConfig(IndexType.SORTED, "age"));  // Sorted index for range queries
     *   imap.addIndex(new IndexConfig(IndexType.HASH, "active")); // Hash index for equality predicates
     * </pre>
     * Index attribute should either have a getter method or be public.
     * You should also make sure to add the indexes before adding
     * entries to this map.
     * <p>
     * <b>Time to Index</b>
     * <p>
     * Indexing time is executed in parallel on each partition by operation threads. The Map
     * is not blocked during this operation.
     * <p>
     * The time taken in proportional to the size of the Map and the number Members.
     * <p>
     * <b>Searches while indexes are being built</b>
     * <p>
     * Until the index finishes being created, any searches for the attribute will use a full Map scan,
     * thus avoiding using a partially built index and returning incorrect results.
     *
     * @param indexConfig Index configuration.
     */
    void addIndex(IndexConfig indexConfig);

    /**
     * Returns LocalMapStats for this map.
     * <p>
     * LocalMapStats are the statistics for the local portion of this
     * distributed map and contains information such as ownedEntryCount
     * backupEntryCount, lastUpdateTime, lockedEntryCount.
     * <p>
     * Since this stats are only for the local portion of this map, if you
     * need the cluster-wide MapStats then you need to get the LocalMapStats
     * from all members of the cluster and combine them.
     * <p>
     * It's guaranteed that the returned {@link LocalMapStats} instance contains
     * an up-to-date statistics. But over the time some parts of the returned
     * instance may become stale while others may be updated. To obtain a fresh
     * up-to-date instance invoke the method one more time.
     *
     * @return this map's local statistics
     */
    LocalMapStats getLocalMapStats();

    /**
     * Applies the user defined {@code EntryProcessor} to the entry mapped by the {@code key}.
     * Returns the object which is the result of the {@link EntryProcessor#process(Map.Entry)} method.
     * <p>
     * The {@code EntryProcessor} may implement the {@link Offloadable} and {@link ReadOnly} interfaces.
     * <p>
     * If the EntryProcessor implements the {@link Offloadable} interface, the processing will be offloaded to the given
     * ExecutorService, allowing unblocking of the partition-thread, which means that other partition-operations
     * may proceed. The key will be locked for the time-span of the processing in order to not generate a write-conflict.
     * In this case the threading looks as follows:
     * <ol>
     * <li>partition-thread (fetch &amp; lock)</li>
     * <li>execution-thread (process)</li>
     * <li>partition-thread (set &amp; unlock, or just unlock if no changes)</li>
     * </ol>
     * If the EntryProcessor implements the Offloadable and ReadOnly interfaces, the processing will be offloaded to the
     * given ExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do
     * any changes to the Entry, the key will NOT be locked for the time-span of the processing. In this case the threading
     * looks as follows:
     * <ol>
     * <li>partition-thread (fetch)</li>
     * <li>execution-thread (process)</li>
     * </ol>
     * In this case, the EntryProcessor.getBackupProcessor() has to return null; otherwise an IllegalArgumentException
     * exception is thrown.
     * <p>
     * If the EntryProcessor implements only ReadOnly without implementing Offloadable, the processing unit will not
     * be offloaded, however, the EntryProcessor will not wait for the lock to be acquired, since the EP will not
     * do any modifications.
     * <p>
     * Using offloading is useful if the EntryProcessor encompasses heavy logic that may stall the partition-thread.
     * <p>
     * If the EntryProcessor implements ReadOnly and modifies the entry it is processing, an UnsupportedOperationException
     * will be thrown.
     * <p>
     * Offloading will not be applied to backup partitions. It is possible to initialize the entry backup processor
     * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
     * The input allows providing context to the entry backup processor, for example the "delta",
     * so that the entry backup processor does not have to calculate the "delta" but it may just apply it.
     * <p>
     * See {@link #submitToKey(Object, EntryProcessor)} for an async version of this method.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If value with {@code key} is not found in memory,
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map.
     * <p>
     * If the entryProcessor updates the entry and write-through
     * persistence mode is configured, before the value is stored
     * in memory, {@link MapStore#store(Object, Object)} is called to
     * write the value into the map store.
     * <p>
     * If the entryProcessor updates the entry's value to null value and
     * write-through persistence mode is configured, before the value is
     * removed from the memory, {@link MapStore#delete(Object)} is
     * called to delete the value from the map store.
     * <p>
     * Any exceptions thrown by the map store fail the operation and are
     * propagated to the caller.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param <R> the entry processor return type
     * @return result of {@link EntryProcessor#process(Map.Entry)}
     * @throws NullPointerException if the specified key is {@code null}
     * @see Offloadable
     * @see ReadOnly
     */
    <R> R executeOnKey(@Nonnull K key,
                       @Nonnull EntryProcessor<K, V, R> entryProcessor);

    /**
     * Applies the user defined {@link EntryProcessor} to the entries mapped by the collection of keys.
     * <p>
     * The operation is not lock-aware. The {@code EntryProcessor} will process the entries no matter if the keys are
     * locked or not. For more details check <b>Entry Processing</b> section on {@link IMap} documentation.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * For each entry not found in memory {@link MapLoader#load(Object)}
     * is invoked to load the value from the map store backing the map.
     * <p>
     * If write-through persistence mode is configured, for each entry
     * updated by the entryProcessor, before the updated value is stored
     * in memory, {@link MapStore#store(Object, Object)} is called to
     * write the value into the map store.
     * <p>
     * If write-through persistence mode is configured, for each entry
     * updated to null value, before the value is removed from the
     * memory, {@link MapStore#delete(Object)} is called to delete the
     * value from the map store.
     * <p>
     * Any exceptions thrown by the map store fail the operation and are
     * propagated to the caller. If an exception happened, the operation might
     * already succeeded on some of the keys.
     * <p>
     * If write-behind persistence mode is
     * configured with write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     * <p>
     * <b>Performance note</b>
     * <p>Keep the state of {@code entryProcessor}
     * small, it will be serialized and one copy will be sent to each member.
     * Additionally, the {@linkplain EntryProcessor#getBackupProcessor() backup
     * processor} will also be serialized once for each affected partition and
     * sent to each backup. For example, in this usage the entire {@code
     * additions} map will be duplicated once for each member and once for each
     * partition and backup:
     * <pre>{@code
     *   HashMap additions = ...;
     *   iMap.executeOnKeys(map.keySet(), entry -> {
     *             Integer updateBy = additions.get(entry.getKey());
     *             entry.setValue(entry.getValue() + updateBy);
     *             return null;
     *         });
     * }</pre>
     *
     * @param keys The keys to execute the entry processor on. Can be empty, in
     *             that case it's a local no-op
     * @param <R>  the entry processor return type
     * @return results of {@link EntryProcessor#process(Map.Entry)}
     * @throws NullPointerException if there's null element in {@code keys}
     */
    <R> Map<K, R> executeOnKeys(@Nonnull Set<K> keys,
                                @Nonnull EntryProcessor<K, V, R> entryProcessor);

    /**
     * Async version of {@link #executeOnKeys}.
     * @param keys the keys to execute the entry processor on. Can be empty, in
     *             that case it's a local no-op
     * @param entryProcessor the processor to process the keys
     * @param <R> return type for entry processor
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon set operation completion
     * @see CompletionStage
     */
    <R> CompletionStage<Map<K, R>> submitToKeys(@Nonnull Set<K> keys,
                               @Nonnull EntryProcessor<K, V, R> entryProcessor);

    /**
     * Applies the user defined {@code EntryProcessor} to the entry mapped by the {@code key}.
     * Returns immediately with a {@link CompletionStage} representing that task.
     * <p>
     * EntryProcessor is not cancellable, so calling CompletionStage.cancel() method
     * won't cancel the operation of EntryProcessor.
     * <p>
     * The EntryProcessor may implement the Offloadable and ReadOnly interfaces.
     * <p>
     * If the EntryProcessor implements the Offloadable interface the processing will be offloaded to the given
     * ExecutorService allowing unblocking the partition-thread, which means that other partition-operations
     * may proceed. The key will be locked for the time-span of the processing in order to not generate a write-conflict.
     * In this case the threading looks as follows:
     * <ol>
     * <li>partition-thread (fetch &amp; lock)</li>
     * <li>execution-thread (process)</li>
     * <li>partition-thread (set &amp; unlock, or just unlock if no changes)</li>
     * </ol>
     * If the EntryProcessor implements the Offloadable and ReadOnly interfaces the processing will be offloaded to the
     * given ExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do
     * any changes to the Entry the key will NOT be locked for the time-span of the processing. In this case the threading
     * looks as follows:
     * <ol>
     * <li>partition-thread (fetch &amp; lock)</li>
     * <li>execution-thread (process)</li>
     * </ol>
     * In this case the EntryProcessor.getBackupProcessor() has to return null; otherwise an IllegalArgumentException
     * exception is thrown.
     * <p>
     * If the EntryProcessor implements only ReadOnly without implementing Offloadable the processing unit will not
     * be offloaded, however, the EntryProcessor will not wait for the lock to be acquired, since the EP will not
     * do any modifications.
     * <p>
     * If the EntryProcessor implements ReadOnly and modifies the entry it is processing a UnsupportedOperationException
     * will be thrown.
     * <p>
     * Using offloading is useful if the EntryProcessor encompasses heavy logic that may stall the partition-thread.
     * <p>
     * Offloading will not be applied to backup partitions. It is possible to initialize the entry backup processor
     * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
     * The input allows providing context to the entry backup processor - for example the "delta"
     * so that the entry backup processor does not have to calculate the "delta" but it may just apply it.
     * <p>
     * See {@link #executeOnKey(Object, EntryProcessor)} for sync version of this method.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * If value with {@code key} is not found in memory
     * {@link MapLoader#load(Object)} is invoked to load the value from
     * the map store backing the map.
     * <p>
     * If the entryProcessor updates the entry and write-through
     * persistence mode is configured, before the value is stored
     * in memory, {@link MapStore#store(Object, Object)} is called to
     * write the value into the map store.
     * <p>
     * If the entryProcessor updates the entry's value to null value and
     * write-through persistence mode is configured, before the value is
     * removed from the memory, {@link MapStore#delete(Object)} is
     * called to delete the value from the map store.
     * <p>
     * Any exception thrown by the map store fail the operation.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param key            key to be processed
     * @param entryProcessor processor to process the key
     * @param <R>            return type for entry processor
     * @return CompletionStage on which client code can block waiting for the
     * operation to complete or register callbacks to be invoked
     * upon set operation completion
     * @see Offloadable
     * @see ReadOnly
     * @see CompletionStage
     */
    <R> CompletionStage<R> submitToKey(@Nonnull K key,
                                       @Nonnull EntryProcessor<K, V, R> entryProcessor);

    /**
     * Applies the user defined {@link EntryProcessor} to the all entries in the map.
     * Returns the results mapped by each key in the map.
     * <p>
     * The operation is not lock-aware. The {@code EntryProcessor} will process the entries no matter if the keys are
     * locked or not. For more details check <b>Entry Processing</b> section on {@link IMap} documentation.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * For each entry not found in memory {@link MapLoader#load(Object)}
     * is invoked to load the value from the map store backing the map.
     * <p>
     * If write-through persistence mode is configured, for each entry
     * updated by the entryProcessor, before the updated value is stored
     * in memory, {@link MapStore#store(Object, Object)} is called to
     * write the value into the map store.
     * <p>
     * If write-through persistence mode is configured, for each entry
     * updated to null value, before the value is removed from the
     * memory, {@link MapStore#delete(Object)} is called to delete the
     * value from the map store.
     * <p>
     * Any exceptions thrown by the map store fail the operation and are
     * propagated to the caller. If an exception happened, the operation might
     * already succeeded on some of the keys.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param entryProcessor processor to process the keys
     * @param <R>            return type for entry processor
     * @return results mapped by entry key
     */
    <R> Map<K, R> executeOnEntries(@Nonnull EntryProcessor<K, V, R> entryProcessor);

    /**
     * Applies the user defined {@link EntryProcessor} to the entries in the map which satisfy provided predicate.
     * Returns the results mapped by each key in the map.
     * <p>
     * The operation is not lock-aware. The {@code EntryProcessor} will process the entries no matter if the keys are
     * locked or not. For more details check <b>Entry Processing</b> section on {@link IMap} documentation.
     *
     * <p><b>Interactions with the map store</b>
     * <p>
     * For each entry not found in memory {@link MapLoader#load(Object)}
     * is invoked to load the value from the map store backing the map.
     * <p>
     * If write-through persistence mode is configured, for each entry
     * updated by the entryProcessor, before the updated value is stored
     * in memory, {@link MapStore#store(Object, Object)} is called to
     * write the value into the map store.
     * <p>
     * If write-through persistence mode is configured, for each entry
     * updated to null value, before the value is removed from the
     * memory, {@link MapStore#delete(Object)} is called to delete the
     * value from the map store.
     * <p>
     * Any exceptions thrown by the map store fail the operation and are
     * propagated to the caller. If an exception happened, the operation might
     * already succeeded on some of the keys.
     * <p>
     * If write-behind persistence mode is configured with
     * write-coalescing turned off,
     * {@link com.hazelcast.map.ReachedMaxSizeException} may be thrown
     * if the write-behind queue has reached its per-node maximum
     * capacity.
     *
     * @param entryProcessor processor to process the keys
     * @param predicate      predicate to filter the entries with
     * @param <R>            return type for entry processor
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @return results mapped by entry key
     */
    <R> Map<K, R> executeOnEntries(@Nonnull EntryProcessor<K, V, R> entryProcessor,
                                   @Nonnull Predicate<K, V> predicate);

    /**
     * Applies the aggregation logic on all map entries and returns the result
     * <p>
     * Fast-Aggregations are the successor of the Map-Reduce Aggregators.
     * They are equivalent to the Map-Reduce Aggregators in most of the use-cases, but instead of running on the Map-Reduce
     * engine they run on the Query infrastructure. Their performance is tens to hundreds times better due to the fact
     * that they run in parallel for each partition and are highly optimized for speed and low memory consumption.
     *
     * @param aggregator aggregator to aggregate the entries with
     * @param <R>        type of the result
     * @return the result of the given type
     * @since 3.8
     */
    <R> R aggregate(@Nonnull Aggregator<? super Map.Entry<K, V>, R> aggregator);

    /**
     * Applies the aggregation logic on map entries filtered with the Predicated and returns the result
     * <p>
     * Fast-Aggregations are the successor of the Map-Reduce Aggregators.
     * They are equivalent to the Map-Reduce Aggregators in most of the use-cases, but instead of running on the Map-Reduce
     * engine they run on the Query infrastructure. Their performance is tens to hundreds times better due to the fact
     * that they run in parallel for each partition and are highly optimized for speed and low memory consumption.
     *
     * @param aggregator aggregator to aggregate the entries with
     * @param predicate  predicate to filter the entries with
     * @param <R>        type of the result
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @return the result of the given type
     * @since 3.8
     */
    <R> R aggregate(@Nonnull Aggregator<? super Map.Entry<K, V>, R> aggregator,
                    @Nonnull Predicate<K, V> predicate);

    /**
     * Applies the projection logic on all map entries and returns the result
     *
     * @param projection projection to transform the entries with (may return null)
     * @param <R>        type of the result
     * @return the result of the given type
     * @since 3.8
     */
    <R> Collection<R> project(@Nonnull Projection<? super Map.Entry<K, V>, R> projection);

    /**
     * Applies the projection logic on map entries filtered with the Predicated and returns the result
     *
     * @param projection projection to transform the entries with (may return null)
     * @param predicate  predicate to filter the entries with
     * @param <R>        type of the result
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @return the result of the given type
     * @since 3.8
     */
    <R> Collection<R> project(@Nonnull Projection<? super Map.Entry<K, V>, R> projection,
                              @Nonnull Predicate<K, V> predicate);

    /**
     * Returns corresponding {@code QueryCache} instance for the supplied {@code name} or null.
     * <p>
     * If there is a previously created {@link QueryCache} with the supplied {@code name} or if a declarative
     * configuration exists for the supplied {@code name} this method returns or creates the instance respectively,
     * otherwise returns null.
     *
     * @param name the name of {@code QueryCache}
     * @return the {@code QueryCache} instance or null if there is no corresponding {@code QueryCacheConfig}
     * @throws NullPointerException if the specified {@code name} is {@code null}
     * @see QueryCache
     * @since 3.8
     */
    QueryCache<K, V> getQueryCache(@Nonnull String name);

    /**
     * Creates an always up-to-date snapshot of this {@code IMap} according to the supplied parameters.
     * <p>
     * If there is a previously created {@link QueryCache} with the supplied {@code name}, this method returns that
     * {@link QueryCache} and ignores {@code predicate} and {@code includeValue} parameters. Otherwise it creates and returns
     * a new {@link QueryCache} instance.
     * <p>
     * Also note that if there exists a {@link com.hazelcast.config.QueryCacheConfig QueryCacheConfig} for the supplied
     * {@code name}, {@code predicate} and {@code includeValue} parameters will overwrite corresponding ones
     * in {@link com.hazelcast.config.QueryCacheConfig}.
     *
     * @param name         the name of {@code QueryCache}
     * @param predicate    the predicate for filtering entries
     * @param includeValue {@code true} if this {@code QueryCache} is allowed to cache values of entries, otherwise {@code false}
     * @return the {@code QueryCache} instance with the supplied {@code name}
     * @throws NullPointerException     if the specified {@code name} or {@code predicate} is {@code null}
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @see QueryCache
     * @since 3.8
     */
    QueryCache<K, V> getQueryCache(@Nonnull String name,
                                   @Nonnull Predicate<K, V> predicate,
                                   boolean includeValue);

    /**
     * Creates an always up-to-date snapshot of this {@code IMap} according to
     * the supplied parameters.
     * <p>
     * If there is a previously created {@link QueryCache} with the supplied
     * {@code name}, this method returns that {@link QueryCache} and ignores
     * {@code listener}, {@code predicate} and {@code includeValue} parameters.
     * Otherwise it creates and returns a new {@link QueryCache} instance.
     * <p>
     * Also note that if there exists a
     * {@link com.hazelcast.config.QueryCacheConfig QueryCacheConfig} for the
     * supplied {@code name}, {@code listener},{@code predicate} and
     * {@code includeValue} parameters will overwrite corresponding ones in
     * {@link com.hazelcast.config.QueryCacheConfig}.
     *
     * @param name         the name of {@code QueryCache}
     * @param listener     the {@code MapListener} which will be used to listen this {@code QueryCache}
     * @param predicate    the predicate for filtering entries
     * @param includeValue {@code true} if this {@code QueryCache} is allowed to cache values of
     *                     entries, otherwise {@code false}
     * @return the {@code QueryCache} instance with the supplied {@code name}
     * @throws NullPointerException     if the specified {@code name} or {@code listener} or {@code predicate}
     *                                  is {@code null}
     * @throws IllegalArgumentException if the predicate is a {@link com.hazelcast.query.PagingPredicate} or is a
     *                                  {@link com.hazelcast.query.PartitionPredicate} that includes a
     *                                  {@link com.hazelcast.query.PagingPredicate}
     * @see QueryCache
     * @since 3.8
     */
    QueryCache<K, V> getQueryCache(@Nonnull String name,
                                   @Nonnull MapListener listener,
                                   @Nonnull Predicate<K, V> predicate,
                                   boolean includeValue);

    /**
     * Updates the TTL (time to live) value of the entry specified by {@code key}
     * with a new TTL value.
     * New TTL value is valid starting from the time this operation is invoked,
     * not since the time the entry was created.
     * If the entry does not exist or is already expired, this call has no effect.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * If there is no entry with key {@code key} or is already expired, this
     * call makes no changes to entries stored in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to
     * the next closest second value.
     *
     * @param key      the key of the map entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative
     *                 means map config default)
     * @param timeunit time unit for the TTL
     * @return {@code true} if the entry exists and its ttl value is changed, {@code false} otherwise
     * @throws NullPointerException if the specified {@code key} or {@code timeunit} is {@code null}.
     * @since 3.11
     */
    boolean setTtl(@Nonnull K key, long ttl, @Nonnull TimeUnit timeunit);

    /**
     * {@inheritDoc}
     *
     * <p> </p>
     * <p>
     *     If the supplied {@code remappingFunction} is a lambda, anonymous class or an inner class,
     *     it would be executed locally. Same would happen if it is not serializable.
     *     This may result in multiple round-trips between hazelcast nodes, and possibly a livelock.
     *</p>
     * <p>
     *     Otherwise (i.e. if it is a top-level class or a member class, and it is serializable), the function <i>may be</i> sent
     *     to the server which owns the key. This results in a single remote call. Also, the function would have exclusive
     *     access to the map entry during its execution.
     *     Note that in this case, the function class must be deployed on all the servers (either physically
     *     or via user-code-deployment).
     * </p>
     * <p>
     *     When this method is invoked using a hazelcast-client instance, the {@code remappingFunction} is always executed locally
     * </p>
     *
     * @since 4.1
     */
    @Override
    V computeIfPresent(@Nonnull K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * {@inheritDoc}
     *
     * <p> </p>
     * <p>
     *     If the supplied {@code mappingFunction} is a lambda, anonymous class or an inner class,
     *     it would be executed locally. Same would happen if it is not serializable.
     *     This may result in two round-trips between hazelcast nodes.
     *</p>
     * <p>
     *     Otherwise (i.e. if it is a top-level class or a member class, and it is serializable), the function <i>may be</i> sent
     *     to the server which owns the key. This results in a single remote call. Also, the function would have exclusive
     *     access to the map entry during its execution.
     *     Note that in this case, the function class must be deployed on all the servers (either physically
     *     or via user-code-deployment).
     * </p>
     * <p>
     *     When this method is invoked using a hazelcast-client instance, the {@code mappingFunction} is always executed locally
     * </p>
     *
     * @since 4.1
     */
    @Override
    V computeIfAbsent(@Nonnull K key, @Nonnull Function<? super K, ? extends V> mappingFunction);

    /**
     * {@inheritDoc}
     *
     * <p> </p>
     * <p>
     *     If the supplied {@code action} is a lambda, anonymous class or an inner class,
     *     it would be executed locally. Same would happen if it is not serializable.
     *     This may result in multiple round-trips between hazelcast nodes, as all map entries
     *     will need to be pulled into the local node
     *</p>
     * <p>
     *     Otherwise (i.e. if it is a top-level class or a member class, and it is serializable), the function <i>may be</i> sent
     *     to the servers which own the partitions/keys. This results in a much less number of remote calls.
     *     Note that in this case, side effects of the {@code action} may not be visible to the local JVM.
     *     If users intend to install the changed value in the map entry, the {@link IMap#executeOnEntries(EntryProcessor)}
     *     method can be used instead
     * </p>
     * <p>
     *     When this method is invoked using a hazelcast-client instance, the {@code action} is always executed locally
     * </p>
     */
    @Override
    default void forEach(@Nonnull BiConsumer<? super K, ? super V> action) {
        ConcurrentMap.super.forEach(action);
    }

    /**
     * {@inheritDoc}
     *
     * <p> </p>
     * <p>
     *     If the supplied {@code remappingFunction} is a lambda, anonymous class or an inner class,
     *     it would be executed locally. Same would happen if it is not serializable.
     *     This may result in multiple round-trips between hazelcast nodes, and possibly a livelock.
     *</p>
     * <p>
     *     Otherwise (i.e. if it is a top-level class or a member class, and it is serializable), the function <i>may be</i> sent
     *     to the server which owns the key. This results in a single remote call. Also, the function would have exclusive
     *     access to the map entry during its execution.
     *     Note that in this case, the function class must be deployed on all the servers (either physically
     *     or via user-code-deployment).
     * </p>
     * <p>
     *     When this method is invoked using a hazelcast-client instance, the {@code remappingFunction} is always executed locally
     * </p>
     *
     * @since 4.1
     */
    @Override
    V compute(@Nonnull K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * {@inheritDoc}
     *
     * <p> </p>
     * <p>
     *     If the supplied {@code remappingFunction} is a lambda, anonymous class or an inner class,
     *     it would be executed locally. Same would happen if it is not serializable.
     *     This may result in multiple round-trips between hazelcast nodes, and possibly a livelock.
     *</p>
     * <p>
     *     Otherwise (i.e. if it is a top-level class or a member class, and it is serializable), the function <i>may be</i> sent
     *     to the server which owns the key. This results in a single remote call. Also, the function would have exclusive
     *     access to the map entry during its execution.
     *     Note that in this case, the function class must be deployed on all the servers (either physically
     *     or via user-code-deployment).
     * </p>
     * <p>
     *     When this method is invoked using a hazelcast-client instance, the {@code remappingFunction} is always executed locally
     * </p>
     *
     * @since 4.1
     */
    @Override
    V merge(@Nonnull K key, @Nonnull V value, @Nonnull BiFunction<? super V, ? super V, ? extends V> remappingFunction);


    /**
     * {@inheritDoc}
     *
     * <p> </p>
     * <p>
     *     If the supplied {@code function} is a lambda, anonymous class or an inner class,
     *     it would be executed locally. Same would happen if it is not serializable.
     *     This may result in multiple round-trips between hazelcast nodes, and possibly a livelock.
     *</p>
     * <p>
     *     Otherwise (i.e. if it is a top-level class or a member class, and it is serializable), the function <i>may be</i> sent
     *     to the server which owns the key. This results in a single remote call. Also, the function would have exclusive
     *     access to the map entry during its execution.
     *     Note that in this case, the function class must be deployed on all the servers (either physically
     *     or via user-code-deployment).
     * </p>
     * <p>
     *     When this method is invoked using a hazelcast-client instance, the {@code function} is always executed locally
     * </p>
     *
     * @since 4.1
     */
    @Override
    default void replaceAll(@Nonnull BiFunction<? super K, ? super V, ? extends V> function) {
        ConcurrentMap.super.replaceAll(function);
    }

    /**
     * Returns an iterator over the entries of the map. It sequentially
     * iterates partitions. It starts to iterate on partition 0, and it
     * finishes the iteration with the last partition (n = 271 by default).
     * The keys are fetched in batches for the constant heap utilization.
     *
     * @return an iterator for the map entries
     * @since 4.2
     */
    @Nonnull
    @Override
    Iterator<Entry<K, V>> iterator();


    /**
     * Returns an iterator over the entries of the map. It sequentially
     * iterates partitions. It starts to iterate on partition 0, and it
     * finishes the iteration with the last partition (n = 271 by default).
     * The keys are fetched in batches for the constant heap utilization.
     *
     * @param fetchSize size for fetching keys in bulk. This size can
     *                  be thought of as page size for iteration. But
     *                  notice that at every fetch only keys are retrieved,
     *                  not values. Values are retrieved on each iterate.
     * @return an iterator for the map entries
     * @since 4.2
     */
    @Nonnull
    Iterator<Entry<K, V>> iterator(int fetchSize);

}

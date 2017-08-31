/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.LockAware;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.LegacyAsyncMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Concurrent, distributed, observable and queryable map.
 * <p>
 * <b>This class is <i>not</i> a general-purpose {@code ConcurrentMap} implementation! While this class implements
 * the {@code Map} interface, it intentionally violates {@code Map's} general contract, which mandates the
 * use of the {@code equals} method when comparing objects. Instead of the {@code equals} method, this
 * implementation compares the serialized byte version of the objects.</b>
 * <p>
 * <b>Gotchas:</b>
 * <ul>
 * <li>Methods, including but not limited to {@code get}, {@code containsKey}, {@code containsValue}, {@code evict},
 * {@code remove}, {@code put}, {@code putIfAbsent}, {@code replace}, {@code lock}, {@code unlock}, do not use
 * {@code hashCode} and {@code equals} implementations of keys. Instead, they use {@code hashCode} and {@code equals}
 * of binary (serialized) forms of the objects.</li>
 * <li>The {@code get} method returns a clone of original values, so modifying the returned value does not change
 * the actual value in the map. You should put the modified value back to make changes visible to all nodes.
 * For additional info, see {@link IMap#get(Object)}.</li>
 * <li>Methods, including but not limited to {@code keySet}, {@code values}, {@code entrySet}, return a collection
 * clone of the values. The collection is <b>NOT</b> backed by the map, so changes to the map are <b>NOT</b> reflected
 * in the collection, and vice-versa.</li>
 * </ul>
 * <p>
 * This class does <em>not</em> allow {@code null} to be used as a key or value.
 * <p>
 * <b>Entry Processing</b>
 * <p>
 * The following operations are lock-aware, since they operate on a single key only.
 * If the key is locked the EntryProcessor will wait until it acquires the lock.
 * <ul>
 * <li>{@link IMap#executeOnKey(Object, EntryProcessor)}</li>
 * <li>{@link IMap#submitToKey(Object, EntryProcessor)}</li>
 * <li>{@link IMap#submitToKey(Object, EntryProcessor, ExecutionCallback)}</li>
 * </ul>
 * There are however following methods that run the EntryProcessor on more than one entry.
 * These operations are not lock-aware.
 * The EntryProcessor will process the entries no matter if they are locked or not.
 * The user may however check if an entry is locked by casting the {@link java.util.Map.Entry} to
 * {@link LockAware} and invoking the {@link LockAware#isLocked()} method.
 * <ul>
 * <li>{@link IMap#executeOnEntries(EntryProcessor)}</li>
 * <li>{@link IMap#executeOnEntries(EntryProcessor, Predicate)}</li>
 * <li>{@link IMap#executeOnKeys(Set, EntryProcessor)}</li>
 * </ul>
 * This applies to both EntryProcessor and backup EntryProcessor.
 *
 * @param <K> key
 * @param <V> value
 * @see java.util.concurrent.ConcurrentMap
 */
public interface IMap<K, V> extends ConcurrentMap<K, V>, LegacyAsyncMap<K, V> {

    /**
     * {@inheritDoc}
     * <p>
     * No atomicity guarantees are given. It could be that in case of failure
     * some of the key/value-pairs get written, while others are not.
     * <p>
     * <b>Warning:</b>
     * <p>
     * If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
     * expire when the initial TTL has elapsed.
     */
    void putAll(Map<? extends K, ? extends V> m);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of the {@code key},
     * not the actual implementations of {@code hashCode} and {@code equals} defined in the {@code key}'s class.
     * The {@code key} will first be searched for in memory. If the key is not found, and if a key is attributed,
     * a {@link MapLoader} will then attempt to load the key.
     *
     * @throws NullPointerException if the specified key is null
     */
    boolean containsKey(Object key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified value is null
     */
    boolean containsValue(Object value);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method returns a clone of the original value, so modifying the returned value does not change
     * the actual value in the map. You should put the modified value back to make changes visible to all nodes.
     * <pre>
     *      V value = map.get(key);
     *      value.updateSomeProperty();
     *      map.put(key, value);
     * </pre>
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @throws NullPointerException if the specified key is null
     */
    V get(Object key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
     * expire when the initial TTL has elapsed.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    V put(K key, V value);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p/>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     *
     * @throws NullPointerException if the specified key is null
     */
    V remove(Object key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    boolean remove(Object key, Object value);

    /**
     * Removes all entries which match with the supplied predicate.
     * <p>
     * If this map has index, matching entries will be found via index search,
     * otherwise they will be found by full-scan.
     * <p>
     * Note that calling this method also removes all entries from callers Near Cache.
     *
     * @param predicate matching entries with this predicate will be removed from this map
     * @throws NullPointerException if the specified predicate is null
     */
    void removeAll(Predicate<K, V> predicate);

    /**
     * Removes the mapping for a key from this map if it is present (optional operation).
     * <p>
     * Unlike {@link #remove(Object)}, this operation does not return
     * the removed value, which avoids the serialization cost of the returned value.
     * <p>
     * If the removed value will not be used, a delete operation
     * is preferred over a remove operation for better performance.
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
     * @param key key whose mapping is to be removed from the map
     * @throws ClassCastException   if the key is of an inappropriate type for this map (optional)
     * @throws NullPointerException if the specified key is null
     */
    void delete(Object key);

    /**
     * If this map has a MapStore, this method flushes
     * all the local dirty entries by calling MapStore.storeAll() and/or MapStore.deleteAll().
     */
    void flush();

    /**
     * Returns the entries for the given keys. If any keys are not present in the Map, it will
     * call {@link MapStore#loadAll(java.util.Collection)}.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * The returned map is <b>NOT</b> backed by the original map,
     * so changes to the original map are <b>NOT</b> reflected in the returned map, and vice-versa.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code keys}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param keys keys to get (keys inside the collection cannot be null)
     * @return map of entries
     * @throws NullPointerException if any of the specified keys are null
     */
    Map<K, V> getAll(Set<K> keys);

    /**
     * Loads all keys into the store. This is a batch load operation so that an implementation can
     * optimize the multiple loads.
     *
     * @param replaceExistingValues when {@code true}, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader
     *                              void loadAll(boolean replaceExistingValues));
     * @since 3.3
     */
    void loadAll(boolean replaceExistingValues);

    /**
     * Loads the given keys. This is a batch load operation so that an implementation can
     * optimize the multiple loads.
     *
     * @param keys                  keys of the values entries to load (keys inside the collection cannot be null)
     * @param replaceExistingValues when {@code true}, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader
     * @since 3.3
     */
    void loadAll(Set<K> keys, boolean replaceExistingValues);

    /**
     * Clears the map and invokes {@link MapStore#deleteAll}deleteAll on MapStore which,
     * if connected to a database, will delete the records from that database.
     * <p>
     * The MAP_CLEARED event is fired for any registered listeners.
     * See {@link com.hazelcast.core.EntryListener#mapCleared(MapEvent)}.
     * <p>
     * To clear a map without calling {@link MapStore#deleteAll}, use {@link #evictAll}.
     *
     * @see #evictAll
     */
    @Override
    void clear();

    /**
     * Asynchronously gets the given key.
     * <pre>
     *   ICompletableFuture future = map.getAsync(key);
     *   // do some other stuff, when ready get the result.
     *   Object value = future.get();
     * </pre>
     * {@link ICompletableFuture#get()} will block until the actual map.get() completes.
     * If the application requires timely response,
     * then {@link ICompletableFuture#get(long, TimeUnit)} can be used.
     * <pre>
     *   try {
     *     ICompletableFuture future = map.getAsync(key);
     *     Object value = future.get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * Additionally, the client can schedule an {@link ExecutionCallback} to be invoked upon
     * completion of the {@code ICompletableFuture} via
     * {@link ICompletableFuture#andThen(ExecutionCallback)} or
     * {@link ICompletableFuture#andThen(ExecutionCallback, Executor)}:
     * <pre>
     *   // assuming a IMap&lt;String, String&gt;
     *   ICompletableFuture&lt;String&gt; future = map.getAsync("a");
     *   future.andThen(new ExecutionCallback&lt;String&gt;() {
     *     public void onResponse(String response) {
     *       // do something with value in response
     *     }
     *
     *     public void onFailure(Throwable t) {
     *       // handle failure
     *     }
     *   });
     * </pre>
     * ExecutionException is never thrown.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the key of the map entry
     * @return ICompletableFuture from which the value of the key can be retrieved
     * @throws NullPointerException if the specified key is null
     * @see ICompletableFuture
     */
    ICompletableFuture<V> getAsync(K key);

    /**
     * Asynchronously puts the given key and value.
     * <pre>
     *   ICompletableFuture future = map.putAsync(key, value);
     *   // do some other stuff, when ready get the result.
     *   Object oldValue = future.get();
     * </pre>
     * ICompletableFuture.get() will block until the actual map.put() completes.
     * If the application requires a timely response,
     * then you can use Future.get(timeout, timeunit).
     * <pre>
     *   try {
     *     ICompletableFuture future = map.putAsync(key, newValue);
     *     Object oldValue = future.get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * Additionally, the client can schedule an {@link ExecutionCallback} to be invoked upon
     * completion of the {@code ICompletableFuture} via {@link ICompletableFuture#andThen(ExecutionCallback)} or
     * {@link ICompletableFuture#andThen(ExecutionCallback, Executor)}:
     * <pre>
     *   // assuming a IMap&lt;String, String&gt;
     *   ICompletableFuture&lt;String&gt; future = map.putAsync("a", "b");
     *   future.andThen(new ExecutionCallback&lt;String&gt;() {
     *     public void onResponse(String response) {
     *       // do something with the old value returned by put operation
     *     }
     *
     *     public void onFailure(Throwable t) {
     *       // handle failure
     *     }
     *   });
     * </pre>
     * ExecutionException is never thrown.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
     * expire when the initial TTL has elapsed.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @return ICompletableFuture from which the old value of the key can be retrieved
     * @throws NullPointerException if the specified key or value is null
     * @see ICompletableFuture
     */
    ICompletableFuture<V> putAsync(K key, V value);

    /**
     * Asynchronously puts the given key and value into this map with a given TTL (time to live) value.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <pre>
     *   ICompletableFuture future = map.putAsync(key, value, ttl, timeunit);
     *   // do some other stuff, when ready get the result
     *   Object oldValue = future.get();
     * </pre>
     * ICompletableFuture.get() will block until the actual map.put() completes.
     * If your application requires a timely response,
     * then you can use Future.get(timeout, timeunit).
     * <pre>
     *   try {
     *     ICompletableFuture future = map.putAsync(key, newValue, ttl, timeunit);
     *     Object oldValue = future.get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * The client can schedule an {@link ExecutionCallback} to be invoked upon
     * completion of the {@code ICompletableFuture} via {@link ICompletableFuture#andThen(ExecutionCallback)} or
     * {@link ICompletableFuture#andThen(ExecutionCallback, Executor)}:
     * <pre>
     *   // assuming a IMap&lt;String, String&gt;
     *   ICompletableFuture&lt;String&gt; future = map.putAsync("a", "b", 5, TimeUnit.MINUTES);
     *   future.andThen(new ExecutionCallback&lt;String&gt;() {
     *     public void onResponse(String response) {
     *       // do something with old value returned by put operation
     *     }
     *
     *     public void onFailure(Throwable t) {
     *       // handle failure
     *     }
     *   });
     * </pre>
     * ExecutionException is never thrown.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      the key of the map entry
     * @param value    the new value of the map entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param timeunit time unit for the TTL
     * @return ICompletableFuture from which the old value of the key can be retrieved
     * @throws NullPointerException if the specified key or value is null
     * @see ICompletableFuture
     */
    ICompletableFuture<V> putAsync(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Asynchronously puts the given key and value.
     * the entry lives forever.
     * Similar to the put operation except that set
     * doesn't return the old value, which is more efficient.
     * <pre>
     *   ICompletableFuture&lt;Void&gt; future = map.setAsync(key, value);
     *   // do some other stuff, when ready wait for completion
     *   future.get();
     * </pre>
     * ICompletableFuture.get() will block until the actual map.set() operation completes.
     * If your application requires a timely response,
     * then you can use ICompletableFuture.get(timeout, timeunit).
     * <pre>
     *   try {
     *     ICompletableFuture&lt;Void&gt; future = map.setAsync(key, newValue);
     *     future.get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * You can also schedule an {@link ExecutionCallback} to be invoked upon
     * completion of the {@code ICompletableFuture} via {@link ICompletableFuture#andThen(ExecutionCallback)} or
     * {@link ICompletableFuture#andThen(ExecutionCallback, Executor)}:
     * <pre>
     *   ICompletableFuture&lt;Void&gt; future = map.setAsync("a", "b");
     *   future.andThen(new ExecutionCallback&lt;String&gt;() {
     *     public void onResponse(Void response) {
     *       // Set operation was completed
     *     }
     *
     *     public void onFailure(Throwable t) {
     *       // handle failure
     *     }
     *   });
     * </pre>
     * ExecutionException is never thrown.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
     * expire when the initial TTL has elapsed.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @return ICompletableFuture on which to block waiting for the operation to complete or
     * register an {@link ExecutionCallback} to be invoked upon completion
     * @throws NullPointerException if the specified key or value is null
     * @see ICompletableFuture
     */
    ICompletableFuture<Void> setAsync(K key, V value);

    /**
     * Asynchronously puts an entry into this map with a given TTL (time to live) value,
     * without returning the old value (which is more efficient than {@code put()}).
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <pre>
     *   ICompletableFuture&lt;Void&gt; future = map.setAsync(key, value, ttl, timeunit);
     *   // do some other stuff, when you want to make sure set operation is complete:
     *   future.get();
     * </pre>
     * ICompletableFuture.get() will block until the actual map set operation completes.
     * If your application requires a timely response,
     * then you can use {@link ICompletableFuture#get(long, TimeUnit)}.
     * <pre>
     *   try {
     *     ICompletableFuture<Void> future = map.setAsync(key, newValue, ttl, timeunit);
     *     future.get(40, TimeUnit.MILLISECOND);
     *   } catch (TimeoutException t) {
     *     // time wasn't enough
     *   }
     * </pre>
     * You can also schedule an {@link ExecutionCallback} to be invoked upon
     * completion of the {@code ICompletableFuture} via {@link ICompletableFuture#andThen(ExecutionCallback)} or
     * {@link ICompletableFuture#andThen(ExecutionCallback, Executor)}:
     * <pre>
     *   ICompletableFuture&lt;Void&gt; future = map.setAsync("a", "b", 5, TimeUnit.MINUTES);
     *   future.andThen(new ExecutionCallback&lt;String&gt;() {
     *     public void onResponse(Void response) {
     *       // Set operation was completed
     *     }
     *
     *     public void onFailure(Throwable t) {
     *       // handle failure
     *     }
     *   });
     * </pre>
     * ExecutionException is never thrown.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      the key of the map entry
     * @param value    the new value of the map entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param timeunit time unit for the TTL
     * @return ICompletableFuture on which client code can block waiting for the operation to complete
     * or provide an {@link ExecutionCallback} to be invoked upon set operation completion
     * @throws NullPointerException if the specified key or value is null
     * @see ICompletableFuture
     */
    ICompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Asynchronously removes the given key, returning an {@link ICompletableFuture} on which
     * the caller can provide an {@link ExecutionCallback} to be invoked upon remove operation
     * completion or block waiting for the operation to complete with {@link ICompletableFuture#get()}.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key The key of the map entry to remove
     * @return {@link ICompletableFuture} from which the value removed from the map can be retrieved
     * @throws NullPointerException if the specified key is null
     * @see ICompletableFuture
     */
    ICompletableFuture<V> removeAsync(K key);

    /**
     * Tries to remove the entry with the given key from this map
     * within the specified timeout value. If the key is already locked by another
     * thread and/or member, then this operation will wait the timeout
     * amount for acquiring the lock.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key      key of the entry
     * @param timeout  maximum time to wait for acquiring the lock for the key
     * @param timeunit time unit for the timeout
     * @return {@code true} if the remove is successful, {@code false} otherwise
     * @throws NullPointerException if the specified key is null
     */
    boolean tryRemove(K key, long timeout, TimeUnit timeunit);

    /**
     * Tries to put the given key and value into this map within a specified
     * timeout value. If this method returns false, it means that
     * the caller thread could not acquire the lock for the key within the
     * timeout duration, thus the put operation is not successful.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
     * expire when the initial TTL has elapsed.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param timeout  maximum time to wait
     * @param timeunit time unit for the timeout
     * @return {@code true} if the put is successful, {@code false} otherwise
     * @throws NullPointerException if the specified key or value is null
     */
    boolean tryPut(K key, V value, long timeout, TimeUnit timeunit);

    /**
     * Puts an entry into this map with a given TTL (time to live) value.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param timeunit time unit for the TTL
     * @return old value of the entry
     * @throws NullPointerException if the specified key or value is null
     */
    V put(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Same as {@link #put(K, V, long, java.util.concurrent.TimeUnit)} except that the MapStore, if defined,
     * will not be called to store/persist the entry.
     * <p>
     * The entry will expire and get evicted after the TTL. If the TTL is 0,
     * then the entry lives forever. If the TTL is negative, then the TTL
     * from the map configuration will be used (default: forever).
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param timeunit time unit for the TTL
     * @throws NullPointerException if the specified key or value is null
     */
    void putTransient(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Note:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * Also, this method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     *
     * @return a clone of the previous value
     * @throws NullPointerException if the specified key or value is null
     */
    V putIfAbsent(K key, V value);

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
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param timeunit time unit for the TTL
     * @return old value of the entry
     * @throws NullPointerException if the specified key or value is null
     */
    V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @throws NullPointerException if any of the specified parameters are null
     */
    boolean replace(K key, V oldValue, V newValue);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * If you have previously set a TTL for the key, the same TTL will be again set on the new value.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    V replace(K key, V value);

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
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 3:</b>
     * <p>
     * If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
     * expire when the initial TTL has elapsed.
     *
     * @param key   key of the entry
     * @param value value of the entry
     * @throws NullPointerException if the specified key or value is null
     */
    void set(K key, V value);

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
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map (0 means infinite, negative means map config default)
     * @param timeunit time unit for the TTL
     * @throws NullPointerException if the specified key or value is null
     */
    void set(K key, V value, long ttl, TimeUnit timeunit);

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
     * @throws NullPointerException if the specified key is null
     */
    void lock(K key);

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
     * @throws NullPointerException if the specified key is null
     */
    void lock(K key, long leaseTime, TimeUnit timeUnit);

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
     * @throws NullPointerException if the specified key is null
     */
    boolean isLocked(K key);

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
     * @throws NullPointerException if the specified key is null
     */
    boolean tryLock(K key);

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
     * @return {@code true} if the lock was acquired, {@code false} if the waiting time elapsed before the lock was acquired
     * @throws NullPointerException if the specified key is null
     */
    boolean tryLock(K key, long time, TimeUnit timeunit)
            throws InterruptedException;

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
     * @return {@code true} if the lock was acquired, {@code false} if the waiting time elapsed before the lock was acquired
     * @throws NullPointerException if the specified key is null
     */
    boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeunit)
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
     * @param key the key to lock
     * @throws NullPointerException         if the specified key is null
     * @throws IllegalMonitorStateException if the current thread does not hold this lock
     */
    void unlock(K key);

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
     * @param key the key to lock
     * @throws NullPointerException if the specified key is null
     */
    void forceUnlock(K key);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     * <p>
     * Note that entries in distributed map are partitioned across
     * the cluster members; each member owns and manages the some portion of the
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
     * @throws NullPointerException          if the listener is null
     * @see #localKeySet()
     * @see MapListener
     */
    String addLocalEntryListener(MapListener listener);

    /**
     * Adds a local entry listener for this map. The added listener will only be
     * listening for the events (add/remove/update/evict) of the locally owned entries.
     * <p>
     * Note that entries in distributed map are partitioned across
     * the cluster members; each member owns and manages the some portion of the
     * entries. Owned entries are called local entries. This
     * listener will be listening for the events of local entries. Let's say
     * your cluster has member1 and member2. On member2 you added a local listener and from
     * member1, you call {@code map.put(key2, value2)}.
     * If the key2 is owned by member2 then the local listener will be
     * notified for the add/update event. Also note that entries can migrate to
     * other nodes for load balancing and/or membership change.
     *
     * @param listener entry listener
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws UnsupportedOperationException if this operation isn't supported, for example on a Hazelcast client
     * @throws NullPointerException          if the listener is null
     * @see #localKeySet()
     * @deprecated please use {@link #addLocalEntryListener(MapListener)} instead
     */
    String addLocalEntryListener(EntryListener listener);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener} sub-interface for that event.
     * The listener will get notified for map events filtered by the given predicate.
     *
     * @param listener     {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws UnsupportedOperationException if this operation isn't supported, for example on a Hazelcast client
     * @throws NullPointerException          if the listener is null
     * @throws NullPointerException          if the predicate is null
     * @see MapListener
     */
    String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds a local entry listener for this map.
     * <p>
     * The added listener will only be listening for the events (add/remove/update/evict) of the locally owned entries.
     * The listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     entry listener
     * @param predicate    predicate for filtering entries
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the listener is null
     * @throws NullPointerException if the predicate is null
     * @deprecated please use {@link #addLocalEntryListener(MapListener, com.hazelcast.query.Predicate, boolean)} instead
     */
    String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds a local entry listener for this map.
     * <p>
     * The added listener will only be listening for the events (add/remove/update/evict) of the locally owned entries.
     * The listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param key          key to listen for
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the listener is null
     * @throws NullPointerException if the predicate is null
     * @see MapListener
     */
    String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Adds a local entry listener for this map.
     * <p>
     * The added listener will only be listening for the events (add/remove/update/evict) of the locally owned entries.
     * The listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     entry listener
     * @param predicate    predicate for filtering entries
     * @param key          key to listen fo
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the listener is null
     * @throws NullPointerException if the predicate is null
     * @deprecated please use {@link #addLocalEntryListener(MapListener, com.hazelcast.query.Predicate, Object, boolean)} instead
     */
    String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Adds an interceptor for this map.
     * <p>
     * Added interceptor will intercept operations and execute user defined methods.
     * They will cancel operations if the user defined method throws an exception.
     *
     * @param interceptor map interceptor
     * @return ID of registered interceptor
     */
    String addInterceptor(MapInterceptor interceptor);

    /**
     * Removes the given interceptor for this map, so it will not intercept operations anymore.
     *
     * @param id registration ID of the map interceptor
     */
    void removeInterceptor(String id);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener} sub-interface for that event.
     *
     * @param listener     {@link MapListener} for this map
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is null
     * @see MapListener
     */
    String addEntryListener(MapListener listener, boolean includeValue);

    /**
     * Adds an entry listener for this map.
     * <p>
     * The listener will get notified for all map add/remove/update/evict events.
     *
     * @param listener     the added entry listener for this map
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is null
     * @deprecated please use {@link #addEntryListener(MapListener, boolean)} instead
     */
    String addEntryListener(EntryListener listener, boolean includeValue);

    /**
     * Removes the specified entry listener.
     * <p>
     * Returns silently if there is no such listener added before.
     *
     * @param id ID of registered listener
     * @return true if registration is removed, false otherwise
     */
    boolean removeEntryListener(String id);

    /**
     * Adds a MapPartitionLostListener.
     * <p>
     * The addPartitionLostListener returns a register ID. This ID is needed to remove the MapPartitionLostListener using the
     * {@link #removePartitionLostListener(String)} method.
     * <p>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     * <p>
     * <b>Warning 1:</b>
     * <p>
     * Please see {@link com.hazelcast.partition.PartitionLostListener} for weaknesses.
     * <p>
     * <b>Warning 2:</b>
     * <p>
     * Listeners registered from HazelcastClient may miss some of the map partition lost events due to design limitations.
     *
     * @param listener the added MapPartitionLostListener
     * @return returns the registration ID for the MapPartitionLostListener
     * @throws java.lang.NullPointerException if listener is null
     * @see #removePartitionLostListener(String)
     * @see com.hazelcast.partition.PartitionLostListener
     */
    String addPartitionLostListener(MapPartitionLostListener listener);

    /**
     * Removes the specified map partition lost listener.
     * <p>
     * Returns silently if there is no such listener was added before.
     *
     * @param id ID of registered listener
     * @return true if registration is removed, false otherwise
     */
    boolean removePartitionLostListener(String id);

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
     * @throws NullPointerException if the specified listener is null
     * @throws NullPointerException if the specified key is null
     * @see MapListener
     */
    String addEntryListener(MapListener listener, K key, boolean includeValue);

    /**
     * Adds the specified entry listener for the specified key.
     * <p>
     * The listener will get notified for all add/remove/update/evict events of the specified key only.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param listener     specified entry listener
     * @param key          key to listen for
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is null
     * @throws NullPointerException if the specified key is null
     * @deprecated please use {@link #addEntryListener(MapListener, Object, boolean)} instead
     */
    String addEntryListener(EntryListener listener, K key, boolean includeValue);

    /**
     * Adds a {@link MapListener} for this map.
     * <p>
     * To receive an event, you should implement a corresponding {@link MapListener} sub-interface for that event.
     *
     * @param listener     the added continuous {@link MapListener} for this map
     * @param predicate    predicate for filtering entries
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is null
     * @throws NullPointerException if the specified predicate is null
     * @see MapListener
     */
    String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds an continuous entry listener for this map.
     * <p>
     * The listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     the added continuous entry listener for this map
     * @param predicate    predicate for filtering entries
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is null
     * @throws NullPointerException if the specified predicate is null
     * @deprecated please use {@link #addEntryListener(MapListener, com.hazelcast.query.Predicate, boolean)} instead
     */
    String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue);

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
     * @throws NullPointerException if the specified listener is null
     * @throws NullPointerException if the specified predicate is null
     * @see MapListener
     */
    String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Adds an continuous entry listener for this map.
     * <p>
     * The listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     the continuous entry listener for this map
     * @param predicate    predicate for filtering entries
     * @param key          key to listen for
     * @param includeValue {@code true} if {@code EntryEvent} should contain the value
     * @return a UUID.randomUUID().toString() which is used as a key to remove the listener
     * @throws NullPointerException if the specified listener is null
     * @throws NullPointerException if the specified predicate is null
     * @deprecated please use {@link #addEntryListener(MapListener, com.hazelcast.query.Predicate, Object, boolean)} instead
     */
    String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Returns the {@code EntryView} for the specified key.
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
     * @throws NullPointerException if the specified key is null
     * @see EntryView
     */
    EntryView<K, V> getEntryView(K key);

    /**
     * Evicts the specified key from this map.
     * <p>
     * If a {@code MapStore} is defined for this map, then the entry is not
     * deleted from the underlying {@code MapStore}, evict only removes
     * the entry from the memory.
     * <p>
     * <b>Warning:</b>
     * <p>
     * This method uses {@code hashCode} and {@code equals} of the binary form of
     * the {@code key}, not the actual implementations of {@code hashCode} and {@code equals}
     * defined in the {@code key}'s class.
     *
     * @param key the specified key to evict from this map
     * @return {@code true} if the key is evicted, {@code false} otherwise
     * @throws NullPointerException if the specified key is null
     */
    boolean evict(K key);

    /**
     * Evicts all keys from this map except the locked ones.
     * <p>
     * If a {@code MapStore} is defined for this map, deleteAll is <strong>not</strong> called by this method.
     * If you do want to deleteAll to be called use the {@link #clear()} method.
     * <p>
     * The EVICT_ALL event is fired for any registered listeners.
     * See {@link com.hazelcast.core.EntryListener#mapEvicted(MapEvent)} .
     *
     * @see #clear()
     * @since 3.3
     */
    void evictAll();

    /**
     * Returns a set clone of the keys contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return a set clone of the keys contained in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> keySet();

    /**
     * Returns a collection clone of the values contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return a collection clone of the values contained in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Collection<V> values();

    /**
     * Returns a {@link Set} clone of the mappings contained in this map.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return a set clone of the keys mappings in this map
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Queries the map based on the specified predicate and
     * returns the keys of matching entries.
     * <p>
     * Specified predicate runs on all members in parallel.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return result key set of the query
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws NullPointerException             if the predicate is null
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> keySet(Predicate predicate);

    /**
     * Queries the map based on the specified predicate and returns the matching entries.
     * <p>
     * Specified predicate runs on all members in parallel.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return result entry set of the query
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws NullPointerException             if the predicate is null
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<Map.Entry<K, V>> entrySet(Predicate predicate);

    /**
     * Queries the map based on the specified predicate and returns the values of matching entries.
     * <p>
     * Specified predicate runs on all members in parallel.
     * <p>
     * <b>Warning:</b>
     * <p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return result value collection of the query
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @throws NullPointerException             if the predicate is null
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Collection<V> values(Predicate predicate);

    /**
     * Returns the locally owned set of keys.
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
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return locally owned keys
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> localKeySet();

    /**
     * Returns the keys of matching locally owned entries.
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
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p>
     * This method is always executed by a distributed query,
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperty#QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria
     * @return keys of matching locally owned entries
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> localKeySet(Predicate predicate);

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
     * you should consider indexing these fields.
     * <pre>
     *   IMap imap = Hazelcast.getMap("employees");
     *   imap.addIndex("age", true);        // ordered, since we have ranged queries for this field
     *   imap.addIndex("active", false);    // not ordered, because boolean field cannot have range
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
     * @param attribute index attribute of value
     * @param ordered   {@code true} if index should be ordered,
     *                  {@code false} otherwise.
     */
    void addIndex(String attribute, boolean ordered);

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
     *
     * @return this map's local statistics
     */
    LocalMapStats getLocalMapStats();

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key.
     * Returns the the object which is result of the process() method of EntryProcessor.
     * <p>
     * The EntryProcessor may implement the Offloadable and ReadOnly interfaces.
     * <p>
     * If the EntryProcessor implements the Offloadable interface the processing will be offloaded to the given
     * ExecutorService allowing unblocking the partition-thread, which means that other partition-operations
     * may proceed. The key will be locked for the time-span of the processing in order to not generate a write-conflict.
     * In this case the threading looks as follows:
     * <ol>
     * <li>partition-thread (fetch & lock)</li>
     * <li>execution-thread (process)</li>
     * <li>partition-thread (set & unlock, or just unlock if no changes)</li>
     * </ol>
     * If the EntryProcessor implements the Offloadable and ReadOnly interfaces the processing will be offloaded to the
     * given ExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do
     * any changes to the Entry the key will NOT be locked for the time-span of the processing. In this case the threading
     * looks as follows:
     * <ol>
     * <li>partition-thread (fetch & lock)</li>
     * <li>execution-thread (process)</li>
     * </ol>
     * In this case the EntryProcessor.getBackupProcessor() has to return null; otherwise an IllegalArgumentException
     * exception is thrown.
     * <p>
     * If the EntryProcessor implements only ReadOnly without implementing Offloadable the processing unit will not
     * be offloaded, however, the EntryProcessor will not wait for the lock to be acquired, since the EP will not
     * do any modifications.
     * <p>
     * Using offloading is useful if the EntryProcessor encompasses heavy logic that may stall the partition-thread.
     * <p>
     * If the EntryProcessor implements ReadOnly and modifies the entry it is processing an UnsupportedOperationException
     * will be thrown.
     * <p>
     * Offloading will not be applied to backup partitions. It is possible to initialize the EntryBackupProcessor
     * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
     * The input allows providing context to the EntryBackupProcessor - for example the "delta"
     * so that the EntryBackupProcessor does not have to calculate the "delta" but it may just apply it.
     *
     * @return result of entry process
     * @throws NullPointerException if the specified key is null
     * @see Offloadable
     * @see ReadOnly
     */
    Object executeOnKey(K key, EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entries mapped by the collection of keys.
     * the results mapped by each key in the collection.
     *
     * @return result of entry process
     * @throws NullPointerException     if the specified key is null
     * @throws IllegalArgumentException if the specified keys set is empty
     */
    Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key with
     * specified ExecutionCallback to listen event status and returns immediately.
     * <p>
     * The EntryProcessor may implement the Offloadable and ReadOnly interfaces.
     * <p>
     * If the EntryProcessor implements the Offloadable interface the processing will be offloaded to the given
     * ExecutorService allowing unblocking the partition-thread, which means that other partition-operations
     * may proceed. The key will be locked for the time-span of the processing in order to not generate a write-conflict.
     * In this case the threading looks as follows:
     * <ol>
     * <li>partition-thread (fetch & lock)</li>
     * <li>execution-thread (process)</li>
     * <li>partition-thread (set & unlock, or just unlock if no changes)</li>
     * </ol>
     * If the EntryProcessor implements the Offloadable and ReadOnly interfaces the processing will be offloaded to the
     * given ExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do
     * any changes to the Entry the key will NOT be locked for the time-span of the processing. In this case the threading
     * looks as follows:
     * <ol>
     * <li>partition-thread (fetch & lock)</li>
     * <li>execution-thread (process)</li>
     * </ol>
     * In this case the EntryProcessor.getBackupProcessor() has to return null; otherwise an IllegalArgumentException
     * exception is thrown.
     * <p>
     * If the EntryProcessor implements only ReadOnly without implementing Offloadable the processing unit will not
     * be offloaded, however, the EntryProcessor will not wait for the lock to be acquired, since the EP will not
     * do any modifications.
     * <p>
     * If the EntryProcessor implements ReadOnly and modifies the entry it is processing an UnsupportedOperationException
     * will be thrown.
     * <p>
     * Using offloading is useful if the EntryProcessor encompasses heavy logic that may stall the partition-thread.
     * <p>
     * Offloading will not be applied to backup partitions. It is possible to initialize the EntryBackupProcessor
     * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
     * The input allows providing context to the EntryBackupProcessor - for example the "delta"
     * so that the EntryBackupProcessor does not have to calculate the "delta" but it may just apply it.
     *
     * @param key            key to be processed
     * @param entryProcessor processor to process the key
     * @param callback       to listen whether operation is finished or not
     * @see Offloadable
     * @see ReadOnly
     */
    void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback);

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key.
     * Returns immediately with a ICompletableFuture representing that task.
     * <p>
     * EntryProcessor is not cancellable, so calling ICompletableFuture.cancel() method
     * won't cancel the operation of EntryProcessor.
     * <p>
     * The EntryProcessor may implement the Offloadable and ReadOnly interfaces.
     * <p>
     * If the EntryProcessor implements the Offloadable interface the processing will be offloaded to the given
     * ExecutorService allowing unblocking the partition-thread, which means that other partition-operations
     * may proceed. The key will be locked for the time-span of the processing in order to not generate a write-conflict.
     * In this case the threading looks as follows:
     * <ol>
     * <li>partition-thread (fetch & lock)</li>
     * <li>execution-thread (process)</li>
     * <li>partition-thread (set & unlock, or just unlock if no changes)</li>
     * </ol>
     * If the EntryProcessor implements the Offloadable and ReadOnly interfaces the processing will be offloaded to the
     * given ExecutorService allowing unblocking the partition-thread. Since the EntryProcessor is not supposed to do
     * any changes to the Entry the key will NOT be locked for the time-span of the processing. In this case the threading
     * looks as follows:
     * <ol>
     * <li>partition-thread (fetch & lock)</li>
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
     * Offloading will not be applied to backup partitions. It is possible to initialize the EntryBackupProcessor
     * with some input provided by the EntryProcessor in the EntryProcessor.getBackupProcessor() method.
     * The input allows providing context to the EntryBackupProcessor - for example the "delta"
     * so that the EntryBackupProcessor does not have to calculate the "delta" but it may just apply it.
     *
     * @param key            key to be processed
     * @param entryProcessor processor to process the key
     * @return ICompletableFuture from which the result of the operation can be retrieved
     * @see Offloadable
     * @see ReadOnly
     * @see ICompletableFuture
     */
    ICompletableFuture submitToKey(K key, EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the all entries in the map.
     * Returns the results mapped by each key in the map.
     */
    Map<K, Object> executeOnEntries(EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entries in the map which satisfies provided predicate.
     * Returns the results mapped by each key in the map.
     */
    Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate);

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
    <R> R aggregate(Aggregator<Map.Entry<K, V>, R> aggregator);

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
     * @return the result of the given type
     * @since 3.8
     */
    <R> R aggregate(Aggregator<Map.Entry<K, V>, R> aggregator, Predicate<K, V> predicate);

    /**
     * Applies the projection logic on all map entries and returns the result
     *
     * @param projection projection to transform the entries with (may return null)
     * @param <R>        type of the result
     * @return the result of the given type
     * @since 3.8
     */
    <R> Collection<R> project(Projection<Map.Entry<K, V>, R> projection);

    /**
     * Applies the projection logic on map entries filtered with the Predicated and returns the result
     *
     * @param projection projection to transform the entries with (may return null)
     * @param predicate  predicate to filter the entries with
     * @param <R>        type of the result
     * @return the result of the given type
     * @since 3.8
     */
    <R> Collection<R> project(Projection<Map.Entry<K, V>, R> projection, Predicate<K, V> predicate);

    /**
     * Executes a predefined aggregation on the maps data set. The {@link com.hazelcast.mapreduce.aggregation.Supplier}
     * is used to either select or to select and extract a (sub-)value. A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the map
     * @param aggregation     the aggregation that is being executed against the map
     * @param <SuppliedValue> the final type emitted from the supplier
     * @param <Result>        the resulting aggregation value type
     * @return the aggregated value
     * @deprecated please use fast-aggregations {@link IMap#aggregate(Aggregator)}
     * or {@link IMap#aggregate(Aggregator, Predicate)} instead
     */
    @Deprecated
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation);

    /**
     * Executes a predefined aggregation on the maps data set.
     * <p>
     * The {@link com.hazelcast.mapreduce.aggregation.Supplier} is used to either select or to select and extract a (sub-)value.
     * A predefined set of aggregations can be found in {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the map
     * @param aggregation     the aggregation that is being executed against the map
     * @param jobTracker      the {@link com.hazelcast.mapreduce.JobTracker} instance to execute the aggregation
     * @param <SuppliedValue> the final type emitted from the supplier
     * @param <Result>        the resulting aggregation value type
     * @return the aggregated value
     * @deprecated please use fast-aggregations {@link IMap#aggregate(Aggregator)}
     * or {@link IMap#aggregate(Aggregator, Predicate)} instead
     */
    @Deprecated
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation,
                                             JobTracker jobTracker);

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
    @Beta
    QueryCache<K, V> getQueryCache(String name);

    /**
     * Creates an always up to date snapshot of this {@code IMap} according to the supplied parameters.
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
     * @throws NullPointerException if the specified {@code name} or {@code predicate} is null
     * @see QueryCache
     * @since 3.8
     */
    @Beta
    QueryCache<K, V> getQueryCache(String name, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Creates an always up to date snapshot of this {@code IMap} according to the supplied parameters.
     * <p>
     * If there is a previously created {@link QueryCache} with the supplied {@code name}, this method returns that
     * {@link QueryCache} and ignores {@code listener}, {@code predicate} and {@code includeValue} parameters.
     * Otherwise it creates and returns a new {@link QueryCache} instance.
     * <p>
     * Also note that if there exists a {@link com.hazelcast.config.QueryCacheConfig QueryCacheConfig} for the supplied
     * {@code name}, {@code listener},{@code predicate} and {@code includeValue} parameters will overwrite corresponding ones
     * in {@link com.hazelcast.config.QueryCacheConfig}.
     *
     * @param name         the name of {@code QueryCache}
     * @param listener     the {@code MapListener} which will be used to listen this {@code QueryCache}
     * @param predicate    the predicate for filtering entries
     * @param includeValue {@code true} if this {@code QueryCache} is allowed to cache values of entries, otherwise {@code false}
     * @return the {@code QueryCache} instance with the supplied {@code name}
     * @throws NullPointerException if the specified {@code name} or {@code listener} or {@code predicate} is null
     * @see QueryCache
     * @since 3.8
     */
    @Beta
    QueryCache<K, V> getQueryCache(String name, MapListener listener, Predicate<K, V> predicate, boolean includeValue);
}

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

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Concurrent, distributed, observable and queryable map.
 * <p/>
 * <p/>
 * <p><b>This class is <i>not</i> a general-purpose <tt>ConcurrentMap</tt> implementation! While this class implements
 * the <tt>Map</tt> interface, it intentionally violates <tt>Map's</tt> general contract, which mandates the
 * use of the <tt>equals</tt> method when comparing objects. Instead of the <tt>equals</tt> method, this implementation
 * compares the serialized byte version of the objects.</b>
 * <p/>
 * <p>
 * <b>Gotchas:</b>
 * <ul>
 * <li>
 * Methods, including but not limited to <tt>get</tt>, <tt>containsKey</tt>,
 * <tt>containsValue</tt>, <tt>evict</tt>, <tt>remove</tt>, <tt>put</tt>,
 * <tt>putIfAbsent</tt>, <tt>replace</tt>, <tt>lock</tt>,
 * <tt>unlock</tt>, do not use <tt>hashCode</tt> and <tt>equals</tt> implementations of keys.
 * Instead, they use <tt>hashCode</tt> and <tt>equals</tt> of binary (serialized) forms of the objects.
 * </li>
 * <li>
 * The <tt>get</tt> method returns a clone of original values, so modifying the returned value does not change
 * the actual value in the map. You should put the modified value back to make changes visible to all nodes.
 * For additional info, see {@link IMap#get(Object)}.
 * </li>
 * <li>
 * Methods, including but not limited to <tt>keySet</tt>, <tt>values</tt>, <tt>entrySet</tt>,
 * return a collection clone of the values. The collection is <b>NOT</b> backed by the map,
 * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
 * </li>
 * </ul>
 * </p>
 * <p>This class does <em>not</em> allow <tt>null</tt> to be used as a key or value.</p>
 *
 * @param <K> key
 * @param <V> value
 * @see java.util.concurrent.ConcurrentMap
 */
public interface IMap<K, V>
        extends ConcurrentMap<K, V>, BaseMap<K, V> {

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.  The <tt>key</tt> will first be searched for in memory. If the key is not
     * found, and if a key is attributed, a {@link MapLoader} will then attempt to load the key.
     * </p>
     *
     * @throws NullPointerException if the specified key is null.
     */
    boolean containsKey(Object key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified value is null.
     */
    boolean containsValue(Object value);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns a clone of the original value, so modifying the returned value does not change
     * the actual value in the map. You should put the modified value back to make changes visible to all nodes.
     * <pre>
     *      V value = map.get(key);
     *      value.updateSomeProperty();
     *      map.put(key, value);
     * </pre>
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     *
     * @throws NullPointerException if the specified key is null.
     */
    V get(Object key);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @throws NullPointerException if the specified key or value is null.
     */
    V put(K key, V value);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     *
     * @throws NullPointerException if the specified key is null.
     */
    V remove(Object key);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @throws NullPointerException if the specified key or value is null.
     */
    boolean remove(Object key, Object value);

    /**
     * Removes the mapping for a key from this map if it is present
     * (optional operation).
     * <p/>
     * <p>Unlike {@link #remove(Object)}, this operation does not return
     * the removed value, which avoids the serialization cost of the returned value.
     * <p/>
     * If the removed value will not be used, a delete operation
     * is preferred over a remove operation for better performance.
     * <p/>
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method breaks the contract of EntryListener.
     * When an entry is removed by delete(), it fires an EntryEvent with a null oldValue.
     * <p/>
     * Also, a listener with predicates will have null values, so only keys can be queried via predicates.
     * <p/>
     *
     * @param key key whose mapping is to be removed from the map.
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map (optional).
     * @throws NullPointerException if the specified key is null.
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
     * <p/>
     * <p><b>Warning:</b></p>
     * The returned map is <b>NOT</b> backed by the original map,
     * so changes to the original map are <b>NOT</b> reflected in the returned map, and vice-versa.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>keys</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     *
     * @param keys keys to get.
     * @return map of entries.
     * @throws NullPointerException if any of the specified keys are null.
     */
    Map<K, V> getAll(Set<K> keys);

    /**
     * Loads all keys into the store. This is a batch load operation so that an implementation can
     * optimize the multiple loads.
     *
     * @param replaceExistingValues when <code>true</code>, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader
     *                              void loadAll(boolean replaceExistingValues));
     * @since 3.3
     */
    void loadAll(boolean replaceExistingValues);

    /**
     * Loads the given keys. This is a batch load operation so that an implementation can
     * optimize the multiple loads.
     *
     * @param keys                  keys of the values entries to load.
     * @param replaceExistingValues when <code>true</code>, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader.
     * @since 3.3
     */
    void loadAll(Set<K> keys, boolean replaceExistingValues);


    /**
     * This method clears the map and invokes {@link MapStore#deleteAll}deleteAll on MapStore which,
     * if connected to a database, will delete the records from that database.
     * <p/>
     * The MAP_CLEARED event is fired for any registered listeners.
     * See {@link com.hazelcast.core.EntryListener#mapCleared(MapEvent)}.
     * <p/>
     * To clear a map without calling {@link MapStore#deleteAll}, use {@link #evictAll}.
     *
     * @see #evictAll
     */
    @Override
    void clear();


    /**
     * Asynchronously gets the given key.
     * <code>
     * Future future = map.getAsync(key);
     * // do some other stuff, when ready get the result.
     * Object value = future.get();
     * </code>
     * Future.get() will block until the actual map.get() completes.
     * If the application requires timely response,
     * then Future.get(timeout, timeunit) can be used.
     * <code>
     * try{
     * Future future = map.getAsync(key);
     * Object value = future.get(40, TimeUnit.MILLISECOND);
     * }catch (TimeoutException t) {
     * // time wasn't enough
     * }
     * </code>
     * ExecutionException is never thrown.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the key of the map entry.
     * @return Future from which the value of the key can be retrieved.
     * @throws NullPointerException if the specified key is null.
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key);

    /**
     * Asynchronously puts the given key and value.
     * <code>
     * Future future = map.putAsync(key, value);
     * // do some other stuff, when ready get the result.
     * Object oldValue = future.get();
     * </code>
     * Future.get() will block until the actual map.put() completes.
     * If the application requires a timely response,
     * then you can use Future.get(timeout, timeunit).
     * <code>
     * try{
     * Future future = map.putAsync(key, newValue);
     * Object oldValue = future.get(40, TimeUnit.MILLISECOND);
     * }catch (TimeoutException t) {
     * // time wasn't enough
     * }
     * </code>
     * ExecutionException is never thrown.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key   the key of the map entry.
     * @param value the new value of the map entry.
     * @return Future from which the old value of the key can be retrieved.
     * @throws NullPointerException if the specified key or value is null.
     * @see java.util.concurrent.Future
     */
    Future<V> putAsync(K key, V value);

    /**
     * Asynchronously puts the given key and value into this map with a given ttl (time to live) value.
     * Entry will expire and get evicted after the ttl. If ttl is 0, then
     * the entry lives forever.
     * <code>
     * Future future = map.putAsync(key, value, ttl, timeunit);
     * // do some other stuff, when ready get the result
     * Object oldValue = future.get();
     * </code>
     * Future.get() will block until the actual map.get() completes.
     * If your application requires a timely response,
     * then you can use Future.get(timeout, timeunit).
     * <code>
     * try{
     * Future future = map.putAsync(key, newValue, ttl, timeunit);
     * Object oldValue = future.get(40, TimeUnit.MILLISECOND);
     * }catch (TimeoutException t) {
     * // time wasn't enough
     * }
     * </code>
     * ExecutionException is never thrown.
     * <p/>
     * <p><b>Warning 1:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning 2:</b></p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      the key of the map entry.
     * @param value    the new value of the map entry.
     * @param ttl      maximum time for this entry to stay in the map.
     *                 0 means infinite.
     * @param timeunit time unit for the ttl.
     * @return Future from which the old value of the key can be retrieved.
     * @throws NullPointerException if the specified key or value is null.
     * @see java.util.concurrent.Future
     */
    Future<V> putAsync(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Asynchronously removes the given key.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key The key of the map entry to remove.
     * @return A {@link java.util.concurrent.Future} from which the value
     * removed from the map can be retrieved.
     * @throws NullPointerException if the specified key is null.
     */
    Future<V> removeAsync(K key);

    /**
     * Tries to remove the entry with the given key from this map
     * within the specified timeout value. If the key is already locked by another
     * thread and/or member, then this operation will wait the timeout
     * amount for acquiring the lock.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     *
     * @param key      key of the entry.
     * @param timeout  maximum time to wait for acquiring the lock
     *                 for the key.
     * @param timeunit time unit for the timeout.
     * @return <tt>true</tt> if the remove is successful, <tt>false</tt>
     * otherwise.
     * @throws NullPointerException if the specified key is null.
     */
    boolean tryRemove(K key, long timeout, TimeUnit timeunit);

    /**
     * Tries to put the given key and value into this map within a specified
     * timeout value. If this method returns false, it means that
     * the caller thread could not acquire the lock for the key within the
     * timeout duration, thus the put operation is not successful.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key      key of the entry.
     * @param value    value of the entry.
     * @param timeout  maximum time to wait.
     * @param timeunit time unit for the timeout.
     * @return <tt>true</tt> if the put is successful, <tt>false</tt> otherwise.
     * @throws NullPointerException if the specified key or value is null.
     */
    boolean tryPut(K key, V value, long timeout, TimeUnit timeunit);

    /**
     * Puts an entry into this map with a given ttl (time to live) value.
     * Entry will expire and get evicted after the ttl. If ttl is 0, then
     * the entry lives forever.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     * <p/>
     * <p><b>Warning 3:</b></p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     * @return old value of the entry
     * @throws NullPointerException if the specified key or value is null
     */
    V put(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Same as {@link #put(K, V, long, java.util.concurrent.TimeUnit)} except that MapStore, if defined,
     * will not be called to store/persist the entry.  If ttl is 0, then
     * the entry lives forever.
     * <p/>
     * <p><b>Warning 1:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning 2:</b></p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map.
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     * @throws NullPointerException if the specified key or value is null
     */
    void putTransient(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Note:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p>
     * Also, this method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     *
     * @return a clone of the previous value.
     * @throws NullPointerException if the specified key or value is null.
     */
    V putIfAbsent(K key, V value);

    /**
     * Puts an entry into this map with a given ttl (time to live) value
     * if the specified key is not already associated with a value.
     * Entry will expire and get evicted after the ttl.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     * <p/>
     * <p><b>Warning 3:</b></p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      key of the entry.
     * @param value    value of the entry.
     * @param ttl      maximum time for this entry to stay in the map.
     * @param timeunit time unit for the ttl.
     * @return old value of the entry.
     * @throws NullPointerException if the specified key or value is null.
     */
    V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @throws NullPointerException if any of the specified parameters are null.
     */
    boolean replace(K key, V oldValue, V newValue);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of the previous value, not the original (identically equal) value
     * previously put into the map.
     * </p>
     *
     * @throws NullPointerException if the specified key or value is null.
     */
    V replace(K key, V value);

    /**
     * Puts an entry into this map.
     * Similar to the put operation except that set
     * doesn't return the old value, which is more efficient.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method breaks the contract of EntryListener.
     * When an entry is updated by set(), it fires an EntryEvent with a null oldValue.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     *
     * @param key   key of the entry.
     * @param value value of the entry.
     * @throws NullPointerException if the specified key or value is null.
     */
    void set(K key, V value);

    /**
     * Puts an entry into this map with a given ttl (time to live) value.
     * Entry will expire and get evicted after the ttl. If ttl is 0, then
     * the entry lives forever. Similar to the put operation except that set
     * doesn't return the old value, which is more efficient.
     * <p/>
     * <p><b>Warning 1:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning 2:</b></p>
     * Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     * @throws NullPointerException if the specified key or value is null
     */
    void set(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Acquires the lock for the specified key.
     * <p>If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * You get a lock whether the value is present in the map or not. Other
     * threads (possibly on other systems) would block on their invoke of
     * <code>lock()</code> until the non-existent key is unlocked. If the lock
     * holder introduces the key to the map, the <code>put()</code> operation
     * is not blocked. If a thread not holding a lock on the non-existent key
     * tries to introduce the key while a lock exists on the non-existent key,
     * the <code>put()</code> operation blocks until it is unlocked.
     * <p/>
     * Scope of the lock is this map only.
     * Acquired lock is only for the key in this map.
     * <p/>
     * Locks are re-entrant so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     * <p/>
     * There is no lock timeout on this method. Locks will be held infinitely.
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key key to lock.
     * @throws NullPointerException if the specified key is null.
     */
    void lock(K key);

    /**
     * Acquires the lock for the specified key for the specified lease time.
     * <p>After lease time, the lock will be released.
     * <p/>
     * <p>If the lock is not available, then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * Scope of the lock is this map only.
     * Acquired lock is only for the key in this map.
     * <p/>
     * Locks are re-entrant, so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key       the key to lock.
     * @param leaseTime time to wait before releasing the lock.
     * @param timeUnit  unit of time to specify lease time.
     * @throws NullPointerException if the specified key is null.
     */
    void lock(K key, long leaseTime, TimeUnit timeUnit);

    /**
     * Checks the lock for the specified key.
     * <p>If the lock is acquired then returns true, else returns false.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the key that is checked for lock.
     * @return <tt>true</tt> if lock is acquired, <tt>false</tt> otherwise.
     * @throws NullPointerException if the specified key is null.
     */
    boolean isLocked(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>If the lock is not available then the current thread
     * doesn't wait and returns false immediately.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the key to lock.
     * @return <tt>true</tt> if lock is acquired, <tt>false</tt> otherwise.
     * @throws NullPointerException if the specified key is null.
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
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key      key to lock in this map.
     * @param time     maximum time to wait for the lock.
     * @param timeunit time unit of the <tt>time</tt> argument.
     * @return <tt>true</tt> if the lock was acquired and <tt>false</tt>
     * if the waiting time elapsed before the lock was acquired.
     * @throws NullPointerException if the specified key is null.
     */
    boolean tryLock(K key, long time, TimeUnit timeunit)
            throws InterruptedException;

    /**
     * Releases the lock for the specified key. It never blocks and
     * returns immediately.
     * <p/>
     * <p>If the current thread is the holder of this lock, then the hold
     * count is decremented.  If the hold count is zero, then the lock
     * is released.  If the current thread is not the holder of this
     * lock, then {@link IllegalMonitorStateException} is thrown.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the key to lock.
     * @throws NullPointerException         if the specified key is null.
     * @throws IllegalMonitorStateException if the current thread does not hold this lock.
     */
    void unlock(K key);

    /**
     * Releases the lock for the specified key regardless of the lock owner.
     * It always successfully unlocks the key, never blocks,
     * and returns immediately.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the key to lock.
     * @throws NullPointerException if the specified key is null.
     */
    void forceUnlock(K key);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     * <p/>
     * Note that entries in distributed map are partitioned across
     * the cluster members; each member owns and manages the some portion of the
     * entries. Owned entries are called local entries. This
     * listener will be listening for the events of local entries. Let's say
     * your cluster has member1 and member2. On member2 you added a local listener and from
     * member1, you call <code>map.put(key2, value2)</code>.
     * If the key2 is owned by member2 then the local listener will be
     * notified for the add/update event. Also note that entries can migrate to
     * other nodes for load balancing and/or membership change.
     *
     * @param listener {@link MapListener} for this map.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see #localKeySet()
     * @see MapListener
     */
    String addLocalEntryListener(MapListener listener);

    /**
     * Adds a local entry listener for this map. The added listener will only be
     * listening for the events (add/remove/update/evict) of the locally owned entries.
     * <p/>
     * Note that entries in distributed map are partitioned across
     * the cluster members; each member owns and manages the some portion of the
     * entries. Owned entries are called local entries. This
     * listener will be listening for the events of local entries. Let's say
     * your cluster has member1 and member2. On member2 you added a local listener and from
     * member1, you call <code>map.put(key2, value2)</code>.
     * If the key2 is owned by member2 then the local listener will be
     * notified for the add/update event. Also note that entries can migrate to
     * other nodes for load balancing and/or membership change.
     *
     * @param listener entry listener.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see #localKeySet()
     * @deprecated use {@link #addLocalEntryListener(MapListener)} instead.
     */
    String addLocalEntryListener(EntryListener listener);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     * Listener will get notified for map events filtered by the given predicate.
     *
     * @param listener     {@link MapListener} for this map.
     * @param predicate    predicate for filtering entries
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see MapListener
     */
    String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds a local entry listener for this map. The added listener will only be
     * listening for the events (add/remove/update/evict) of the locally owned entries.
     * Listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     entry listener
     * @param predicate    predicate for filtering entries
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @deprecated use {@link #addLocalEntryListener(MapListener, com.hazelcast.query.Predicate, boolean)}
     */
    String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds a local entry listener for this map. The added listener will only be
     * listening for the events (add/remove/update/evict) of the locally owned entries.
     * Listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     {@link MapListener} for this map.
     * @param predicate    predicate for filtering entries.
     * @param key          key to listen for.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see MapListener
     */
    String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue);


    /**
     * Adds a local entry listener for this map. The added listener will only be
     * listening for the events (add/remove/update/evict) of the locally owned entries.
     * Listener will get notified for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     entry listener.
     * @param predicate    predicate for filtering entries.
     * @param key          key to listen for.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @deprecated use {@link #addLocalEntryListener(MapListener, com.hazelcast.query.Predicate, boolean)} instead
     */
    String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Adds an interceptor for this map. Added interceptor will intercept operations
     * and execute user defined methods and will cancel operations if user defined method throw exception.
     * <p/>
     *
     * @param interceptor map interceptor.
     * @return id of registered interceptor.
     */
    String addInterceptor(MapInterceptor interceptor);

    /**
     * Removes the given interceptor for this map so it will not intercept operations anymore.
     * <p/>
     *
     * @param id registration id of the map interceptor.
     */
    void removeInterceptor(String id);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     *
     * @param listener     {@link MapListener} for this map.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see MapListener
     */
    String addEntryListener(MapListener listener, boolean includeValue);

    /**
     * Adds an entry listener for this map. Listener will get notified
     * for all map add/remove/update/evict events.
     *
     * @param listener     the added entry listener for this map.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @deprecated use {@link #addEntryListener(MapListener, boolean)} instead.
     */
    String addEntryListener(EntryListener listener, boolean includeValue);

    /**
     * Removes the specified entry listener.
     * Returns silently if there is no such listener added before.
     *
     * @param id id of registered listener.
     * @return true if registration is removed, false otherwise.
     */
    boolean removeEntryListener(String id);


    /**
     * Adds a MapPartitionLostListener.
     * <p/>
     * The addPartitionLostListener returns a register-id. This id is needed to remove the MapPartitionLostListener using the
     * {@link #removePartitionLostListener(String)} method.
     * <p/>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     * IMPORTANT: Please @see com.hazelcast.partition.PartitionLostListener for weaknesses.
     * IMPORTANT: Listeners registered from HazelcastClient may miss some of the map partition lost events due
     * to design limitations.
     *
     * @param listener the added MapPartitionLostListener.
     * @return returns the registration id for the MapPartitionLostListener.
     * @throws java.lang.NullPointerException if listener is null.
     * @see #removePartitionLostListener(String)
     */
    String addPartitionLostListener(MapPartitionLostListener listener);

    /**
     * Removes the specified map partition lost listener.
     * Returns silently if there is no such listener added before.
     *
     * @param id id of registered listener.
     * @return true if registration is removed, false otherwise.
     */
    boolean removePartitionLostListener(String id);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param listener     {@link MapListener} for this map.
     * @param key          key to listen for.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @throws NullPointerException if the specified key is null.
     * @see MapListener
     */
    String addEntryListener(MapListener listener, K key, boolean includeValue);

    /**
     * Adds the specified entry listener for the specified key.
     * The listener will get notified for all
     * add/remove/update/evict events of the specified key only.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param listener     specified entry listener.
     * @param key          key to listen for.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @throws NullPointerException if the specified key is null.
     * @deprecated use {@link #addEntryListener(MapListener, Predicate, Object, boolean)} instead.
     */
    String addEntryListener(EntryListener listener, K key, boolean includeValue);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     *
     * @param listener     the added continuous {@link MapListener} for this map.
     * @param predicate    predicate for filtering entries.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see MapListener
     */
    String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds an continuous entry listener for this map. Listener will get notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     the added continuous entry listener for this map.
     * @param predicate    predicate for filtering entries.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @deprecated use {@link #addEntryListener(MapListener, Predicate, boolean)} instead.
     */
    String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Adds a {@link MapListener} for this map. To receive an event, you should
     * implement a corresponding {@link MapListener} sub-interface for that event.
     *
     * @param listener     the continuous {@link MapListener} for this map.
     * @param predicate    predicate for filtering entries.
     * @param key          key to listen for.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @see MapListener
     */
    String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Adds an continuous entry listener for this map. Listener will get notified
     * for map add/remove/update/evict events filtered by the given predicate.
     *
     * @param listener     the continuous entry listener for this map.
     * @param predicate    predicate for filtering entries.
     * @param key          key to listen for.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     * @deprecated use {@link #addEntryListener(MapListener, Object, boolean)}
     */
    String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue);

    /**
     * Returns the <tt>EntryView</tt> for the specified key.
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns a clone of original mapping, modifying the returned value does not change
     * the actual value in the map. One should put modified value back to make changes visible to all nodes.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the key of the entry.
     * @return <tt>EntryView</tt> of the specified key.
     * @throws NullPointerException if the specified key is null.
     * @see EntryView
     */
    EntryView<K, V> getEntryView(K key);

    /**
     * Evicts the specified key from this map. If
     * a <tt>MapStore</tt> is defined for this map, then the entry is not
     * deleted from the underlying <tt>MapStore</tt>, evict only removes
     * the entry from the memory.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of the binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in the <tt>key</tt>'s class.
     *
     * @param key the specified key to evict from this map.
     * @return <tt>true</tt> if the key is evicted, <tt>false</tt> otherwise.
     * @throws NullPointerException if the specified key is null.
     */
    boolean evict(K key);

    /**
     * Evicts all keys from this map except the locked ones.
     * <p/>
     * If a <tt>MapStore</tt> is defined for this map, deleteAll is <strong>not</strong> called by this method.
     * If you do want to deleteAll to be called use the {@link #clear()} method.
     * <p/>
     * The EVICT_ALL event is fired for any registered listeners.
     * See {@link com.hazelcast.core.EntryListener#mapEvicted(MapEvent)} .
     *
     * @see #clear()
     * @since 3.3
     */
    void evictAll();

    /**
     * Returns a set clone of the keys contained in this map.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p/>
     * On the server side this method is executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return a set clone of the keys contained in this map.
     * @throws QueryResultSizeExceededException on server side if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> keySet();

    /**
     * Returns a collection clone of the values contained in this map.
     * <p/>
     * <p><b>Warning:</b></p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     * <p/>
     * On the server side this method is executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return a collection clone of the values contained in this map
     * @throws QueryResultSizeExceededException on server side if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Collection<V> values();

    /**
     * Returns a {@link Set} clone of the mappings contained in this map.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p/>
     * On the server side this method is executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return a set clone of the keys mappings in this map
     * @throws QueryResultSizeExceededException on server side if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Queries the map based on the specified predicate and
     * returns the keys of matching entries.
     * <p/>
     * Specified predicate runs on all members in parallel.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p/>
     * This method is always executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria.
     * @return result key set of the query.
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> keySet(Predicate predicate);

    /**
     * Queries the map based on the specified predicate and
     * returns the matching entries.
     * <p/>
     * Specified predicate runs on all members in parallel.
     * <p/>
     * <p><b>Warning:</b></p>
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     * <p/>
     * This method is always executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria.
     * @return result entry set of the query.
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Set<Map.Entry<K, V>> entrySet(Predicate predicate);

    /**
     * Queries the map based on the specified predicate and
     * returns the values of matching entries.
     * <p/>
     * Specified predicate runs on all members in parallel.
     * <p/>
     * <p><b>Warning:</b></p>
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     * <p/>
     * This method is always executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria.
     * @return result value collection of the query.
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Collection<V> values(Predicate predicate);

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
     * <p/>
     * On the server side this method is executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @return locally owned keys.
     * @throws QueryResultSizeExceededException on server side if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> localKeySet();

    /**
     * Returns the keys of matching locally owned entries.
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
     * <p/>
     * This method is always executed by a distributed query
     * so it may throw a {@link QueryResultSizeExceededException}
     * if {@link GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT} is configured.
     *
     * @param predicate specified query criteria.
     * @return keys of matching locally owned entries.
     * @throws QueryResultSizeExceededException if query result size limit is exceeded
     * @see GroupProperties#PROP_QUERY_RESULT_SIZE_LIMIT
     */
    Set<K> localKeySet(Predicate predicate);

    /**
     * Adds an index to this map for the specified entries so
     * that queries can run faster.
     * <p/>
     * Let's say your map values are Employee objects.
     * <pre>
     *   public class Employee implements Serializable {
     *       private boolean active = false;
     *       private int age;
     *       private String name = null;
     *       // other fields.
     *
     *       // getters setter
     *
     *   }
     * </pre>
     * <p/>
     * If you are querying your values mostly based on age and active then
     * you should consider indexing these fields.
     * <pre>
     *   IMap imap = Hazelcast.getMap("employees");
     *   imap.addIndex("age", true);        // ordered, since we have ranged queries for this field
     *   imap.addIndex("active", false);    // not ordered, because boolean field cannot have range
     * </pre>
     * <p/>
     * Index attribute should either have a getter method or be public.
     * You should also make sure to add the indexes before adding
     * entries to this map.
     * <p/>
     * <h3>Time to Index</h3>
     * Indexing time is executed in parallel on each partition by operation threads. The Map
     * is not blocked during this operation.
     * <p/>
     * The time taken in proportional to the size of the Map and the number Members.
     * <p/>
     * <h3>Searches while indexes are being built</h3>
     * Until the index finishes being created, any searches for the attribute will use a full Map scan,
     * thus avoiding using a partially built index and returning incorrect results.
     *
     * @param attribute index attribute of value
     * @param ordered   <tt>true</tt> if index should be ordered,
     *                  <tt>false</tt> otherwise.
     */
    void addIndex(String attribute, boolean ordered);

    /**
     * Returns LocalMapStats for this map.
     * LocalMapStats is the statistics for the local portion of this
     * distributed map and contains information such as ownedEntryCount
     * backupEntryCount, lastUpdateTime, lockedEntryCount.
     * <p/>
     * Since this stats are only for the local portion of this map, if you
     * need the cluster-wide MapStats then you need to get the LocalMapStats
     * from all members of the cluster and combine them.
     *
     * @return this map's local statistics.
     */
    LocalMapStats getLocalMapStats();

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key.
     * Returns the the object which is result of the process() method of EntryProcessor.
     * <p/>
     *
     * @return result of entry process.
     * @throws NullPointerException if the specified key is null
     */
    Object executeOnKey(K key, EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entries mapped by the collection of keys.
     * the results mapped by each key in the collection.
     * <p/>
     *
     * @return result of entry process.
     * @throws NullPointerException if the specified key is null.
     */
    Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key with
     * specified ExecutionCallback to listen event status and returns immediately.
     *
     * @param key            key to be processed.
     * @param entryProcessor processor to process the key.
     * @param callback       to listen whether operation is finished or not.
     */
    void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback);

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key.
     * Returns immediately with a Future representing that task.
     * <p/>
     * EntryProcessor is not cancellable, so calling Future.cancel() method won't cancel the operation of EntryProcessor.
     *
     * @param key            key to be processed
     * @param entryProcessor processor to process the key
     * @return Future from which the result of the operation can be retrieved.
     * @see java.util.concurrent.Future
     */
    Future submitToKey(K key, EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the all entries in the map.
     * Returns the results mapped by each key in the map.
     * <p/>
     */
    Map<K, Object> executeOnEntries(EntryProcessor entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entries in the map which satisfies provided predicate.
     * Returns the results mapped by each key in the map.
     * <p/>
     */
    Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate);

    /**
     * Executes a predefined aggregation on the maps data set. The {@link com.hazelcast.mapreduce.aggregation.Supplier}
     * is used to either select or to select and extract a (sub-)value. A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the map.
     * @param aggregation     the aggregation that is being executed against the map.
     * @param <SuppliedValue> the final type emitted from the supplier.
     * @param <Result>        the resulting aggregation value type.
     * @return the aggregated value.
     */
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation);

    /**
     * Executes a predefined aggregation on the maps data set. The {@link com.hazelcast.mapreduce.aggregation.Supplier}
     * is used to either select or to select and extract a (sub-)value. A predefined set of aggregations can be found in
     * {@link com.hazelcast.mapreduce.aggregation.Aggregations}.
     *
     * @param supplier        the supplier to select and / or extract a (sub-)value from the map.
     * @param aggregation     the aggregation that is being executed against the map.
     * @param jobTracker      the {@link com.hazelcast.mapreduce.JobTracker} instance to execute the aggregation.
     * @param <SuppliedValue> the final type emitted from the supplier.
     * @param <Result>        the resulting aggregation value type.
     * @return the aggregated value
     */
    <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                             Aggregation<K, SuppliedValue, Result> aggregation,
                                             JobTracker jobTracker);
}

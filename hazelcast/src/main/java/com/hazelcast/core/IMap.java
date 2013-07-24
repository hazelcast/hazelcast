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

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
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
 *
 * <p/>
 * <p><b>This class is <i>not</i> a general-purpose <tt>ConcurrentMap</tt> implementation! While this class implements
 * the <tt>Map</tt> interface, it intentionally violates <tt>Map's</tt> general contract, which mandates the
 * use of the <tt>equals</tt> method when comparing objects. Instead of the <tt>equals</tt> method this implementation
 * compares the serialized byte version of the objects.</b>
 * <p/>
 * <p>
 * <b>Gotchas:</b>
 * <ul>
 * <li>
 * Methods, including but not limited to <tt>get</tt>, <tt>containsKey</tt>,
 * <tt>containsValue</tt>, <tt>evict</tt>, <tt>remove</tt>, <tt>put</tt>,
 * <tt>putIfAbsent</tt>, <tt>replace</tt>, <tt>lock</tt>,
 * <tt>unlock</tt>, do not use <tt>hashCode</tt> and <tt>equals</tt> implementations of keys,
 * instead they use <tt>hashCode</tt> and <tt>equals</tt> of binary (serialized) forms of the objects.
 * </li>
 * <li>
 * <tt>get</tt> method returns a clone of original values, modifying the returned value does not change
 * the actual value in the map. One should put modified value back to make changes visible to all nodes.
 * For additional info see {@link IMap#get(Object)}.
 * </li>
 * </li>
 * <li>
 * Methods, including but not limited to <tt>keySet</tt>, <tt>values</tt>, <tt>entrySet</tt>,
 * return a collection clone of the values. The collection is <b>NOT</b> backed by the map,
 * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
 * </li>
 * </ul>
 * </p>
 *
 * @param <K> key
 * @param <V> value
 * @see java.util.concurrent.ConcurrentMap
 */
public interface IMap<K, V> extends ConcurrentMap<K, V>, BaseMap<K, V> {

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>                                                                                      ˆ
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * </p>
     */
    boolean containsKey(Object key);

    /**
     * {@inheritDoc}
     */
    boolean containsValue(Object value);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns a clone of original value, modifying the returned value does not change
     * the actual value in the map. One should put modified value back to make changes visible to all nodes.
     * <pre>
     *      V value = map.get(key);
     *      value.updateSomeProperty();
     *      map.put(key, value);
     * </pre>
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * <p/>
     */
    V get(Object key);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     */
    V put(K key, V value);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * <p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * </p>
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     */
    V remove(Object key);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     */
    boolean remove(Object key, Object value);


    /**
     * Removes the mapping for a key from this map if it is present
     * (optional operation).
     *
     * <p>Differently from {@link #remove(Object)}; this operation does not return
     * removed value to avoid serialization cost of returned value.
     *
     * If the removed value will not be used, delete operation
     * should be preferred over remove operation for a better performance.
     *
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key key whose mapping is to be removed from the map
     * @throws ClassCastException if the key is of an inappropriate type for
     *         this map (optional)
     * @throws NullPointerException if the specified key is null and this
     *         map does not permit null keys (optional)
     */
    void delete(Object key);

    /**
     * If this map has a MapStore this method flushes
     * all the local dirty entries by calling MapStore.storeAll() and/or MapStore.deleteAll()
     */
    void flush();

    /**
     * Returns the entries for the given keys.
     * <p/>
     * <p><b>Warning:</b></p>
     * The returned map is <b>NOT</b> backed by the original map,
     * so changes to the original map are <b>NOT</b> reflected in the returned map, and vice-versa.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>keys</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param keys keys to get
     * @return map of entries
     */
    Map<K, V> getAll(Set<K> keys);

    /**
     * Asynchronously gets the given key.
     * <code>
     * Future future = map.getAsync(key);
     * // do some other stuff, when ready get the result
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
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key the key of the map entry
     * @return Future from which the value of the key can be retrieved.
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key);

    /**
     * Asynchronously puts the given key and value.
     * <code>
     * Future future = map.putAsync(key, value);
     * // do some other stuff, when ready get the result
     * Object oldValue = future.get();
     * </code>
     * Future.get() will block until the actual map.get() completes.
     * If the application requires timely response,
     * then Future.get(timeout, timeunit) can be used.
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
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @return Future from which the old value of the key can be retrieved.
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
     * If the application requires timely response,
     * then Future.get(timeout, timeunit) can be used.
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
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @param ttl      maximum time for this entry to stay in the map
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     * @return Future from which the old value of the key can be retrieved.
     * @see java.util.concurrent.Future
     */
    Future<V> putAsync(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Asynchronously removes the given key.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key The key of the map entry to remove.
     * @return A {@link java.util.concurrent.Future} from which the value
     *         removed from the map can be retrieved.
     */
    Future<V> removeAsync(K key);

    /**
     * Tries to remove the entry with the given key from this map
     * within specified timeout value. If the key is already locked by another
     * thread and/or member, then this operation will wait timeout
     * amount for acquiring the lock.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     *
     * @param key      key of the entry
     * @param timeout  maximum time to wait for acquiring the lock
     *                 for the key
     * @param timeunit time unit for the timeout
     * @return <tt>true</tt> if the remove is successful, <tt>false</tt>
     *         otherwise.
     */
    boolean tryRemove(K key, long timeout, TimeUnit timeunit) ;

    /**
     * Tries to put the given key, value into this map within specified
     * timeout value. If this method returns false, it means that
     * the caller thread couldn't acquire the lock for the key within
     * timeout duration, thus put operation is not successful.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param timeout  maximum time to wait
     * @param timeunit time unit for the timeout
     * @return <tt>true</tt> if the put is successful, <tt>false</tt>
     *         otherwise.
     */
    boolean tryPut(K key, V value, long timeout, TimeUnit timeunit);

    /**
     * Puts an entry into this map with a given ttl (time to live) value.
     * Entry will expire and get evicted after the ttl. If ttl is 0, then
     * the entry lives forever.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     * @return old value of the entry
     */
    V put(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Same as {@link #put(K, V, long, TimeUnit)} but MapStore, if defined,
     * will not be called to store/persist the entry.  If ttl is 0, then
     * the entry lives forever.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map.
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     */
    void putTransient(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     */
    V putIfAbsent(K key, V value);

    /**
     * Puts an entry into this map with a given ttl (time to live) value
     * if the specified key is not already associated with a value.
     * Entry will expire and get evicted after the ttl.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map
     * @param timeunit time unit for the ttl
     * @return old value of the entry
     */
    V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     */
    boolean replace(K key, V oldValue, V newValue);

    /**
     * {@inheritDoc}
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     * <p/>
     * <p><b>Warning-2:</b></p>
     * <p>
     * This method returns a clone of previous value, not the original (identically equal) value
     * previously put into map.
     * </p>
     */
    V replace(K key, V value);

    /**
     * Puts an entry into this map with a given ttl (time to live) value.
     * Similar to put operation except that set
     * doesn't return the old value which is more efficient.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     */
    void set(K key, V value);

    /**
     * Puts an entry into this map with a given ttl (time to live) value.
     * Entry will expire and get evicted after the ttl. If ttl is 0, then
     * the entry lives forever. Similar to put operation except that set
     * doesn't return the old value which is more efficient.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key      key of the entry
     * @param value    value of the entry
     * @param ttl      maximum time for this entry to stay in the map
     *                 0 means infinite.
     * @param timeunit time unit for the ttl
     * @return old value of the entry
     */
    void set(K key, V value, long ttl, TimeUnit timeunit);

    /**
     * Acquires the lock for the specified key.
     * <p>If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * Scope of the lock is this map only.
     * Acquired lock is only for the key in this map.
     * <p/>
     * Locks are re-entrant so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to lock.
     */
    void lock(K key);

    /**
     * Acquires the lock for the specified key for the specified lease time.
     * <p>After lease time, lock will be released..
     * <p/>
     * <p>If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * Scope of the lock is this map only.
     * Acquired lock is only for the key in this map.
     * <p/>
     * Locks are re-entrant so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to lock.
     * @param leaseTime time to wait before releasing the lock.
     * @param timeUnit unit of time to specify lease time.
     */
    void lock(K key, long leaseTime, TimeUnit timeUnit);

    /**
     * Checks the lock for the specified key.
     * <p>If the lock is acquired then returns true, else false.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to lock to be checked.
     * @return <tt>true</tt> if lock is acquired, <tt>false</tt> otherwise.
     */
    boolean isLocked(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>If the lock is not available then the current thread
     * doesn't wait and returns false immediately.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to lock.
     * @return <tt>true</tt> if lock is acquired, <tt>false</tt> otherwise.
     */
    boolean tryLock(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>The lock is acquired by the current thread; or
     * <li>The specified waiting time elapses
     * </ul>
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key      key to lock in this map
     * @param time     maximum time to wait for the lock
     * @param timeunit time unit of the <tt>time</tt> argument.
     * @return <tt>true</tt> if the lock was acquired and <tt>false</tt>
     *         if the waiting time elapsed before the lock was acquired.
     */
    boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException;

    /**
     * Releases the lock for the specified key. It never blocks and
     * returns immediately.
     * <p/>
     * <p>If the current thread is the holder of this lock then the hold
     * count is decremented.  If the hold count is now zero then the lock
     * is released.  If the current thread is not the holder of this
     * lock then {@link IllegalMonitorStateException} is thrown.
     * <p/>
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to lock.
     * @throws IllegalMonitorStateException if the current thread does not hold this lock
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
     * Adds a local entry listener for this map. Added listener will be only
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
     * @param listener entry listener
     * @see #localKeySet()
     */
    String addLocalEntryListener(EntryListener<K, V> listener);

    /**
     * Adds an interceptor for this map. Added interceptor will intercept operations
     * and execute user defined methods and will cancel operations if user defined method throw exception.
     * <p/>
     *
     * @param interceptor map interceptor
     * @return id of registered interceptor
     */
    String addInterceptor(MapInterceptor interceptor);

    /**
     * Removes the given interceptor for this map. So it will not intercept operations anymore.
     * <p/>
     *
     * @param id registration id of map interceptor
     */
    void removeInterceptor(String id);

    /**
     * Adds an entry listener for this map. Listener will get notified
     * for all map add/remove/update/evict events.
     *
     * @param listener     entry listener
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     */
    String addEntryListener(EntryListener<K, V> listener, boolean includeValue);

    /**
     * Removes the specified entry listener
     * Returns silently if there is no such listener added before.
     *
     *
     * @param id id of registered listener
     *
     * @return true if registration is removed, false otherwise
     */
    boolean removeEntryListener(String id);

    /**
     * Adds the specified entry listener for the specified key.
     * The listener will get notified for all
     * add/remove/update/evict events of the specified key only.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param listener     entry listener
     * @param key          key to listen
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     */
    String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue);

    /**
     * Adds an continuous entry listener for this map. Listener will get notified
     * for map add/remove/update/evict events filtered by given predicate.
     *
     * @param listener  entry listener
     * @param predicate predicate for filtering entries
     */
    String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue);

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
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key of the entry
     * @return <tt>EntryView</tt> of the specified key
     * @see EntryView
     */
    EntryView<K,V> getEntryView(K key);

    /**
     * Evicts the specified key from this map. If
     * a <tt>MapStore</tt> defined for this map, then the entry is not
     * deleted from the underlying <tt>MapStore</tt>, evict only removes
     * the entry from the memory.
     * <p/>
     * <p><b>Warning:</b></p>
     * This method uses <tt>hashCode</tt> and <tt>equals</tt> of binary form of
     * the <tt>key</tt>, not the actual implementations of <tt>hashCode</tt> and <tt>equals</tt>
     * defined in <tt>key</tt>'s class.
     *
     * @param key key to evict
     * @return <tt>true</tt> if the key is evicted, <tt>false</tt> otherwise.
     */
    boolean evict(K key);

    /**
     * Returns a set clone of the keys contained in this map.
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     *
     * @return a set clone of the keys contained in this map
     */
    Set<K> keySet();

    /**
     * Returns a collection clone of the values contained in this map.
     * The collection is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the collection, and vice-versa.
     *
     * @return a collection clone of the values contained in this map
     */
    Collection<V> values();

    /**
     * Returns a {@link Set} clone of the mappings contained in this map.
     * The set is <b>NOT</b> backed by the map,
     * so changes to the map are <b>NOT</b> reflected in the set, and vice-versa.
     *
     * @return a set clone of the keys mappings in this map
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
     *
     * @param predicate query criteria
     * @return result key set of the query
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
     *
     * @param predicate query criteria
     * @return result entry set of the query
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
     *
     * @param predicate query criteria
     * @return result value collection of the query
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
     *
     * @return locally owned keys.
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
     *
     * @param predicate query criteria
     * @return keys of matching locally owned entries.
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
     *
     * @param attribute attribute of value
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
     */
    Object executeOnKey(K key, EntryProcessor entryProcessor);


    /**
     * Applies the user defined EntryProcessor to the all entries in the map.
     * Returns the results mapped by each key in the map.
     * <p/>
     *
     */
    Map<K,Object> executeOnEntries(EntryProcessor entryProcessor);

}

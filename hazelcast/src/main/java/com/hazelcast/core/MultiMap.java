/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A specialized map whose keys can be associated with multiple values.
 *
 * @author oztalip
 */
public interface MultiMap<K, V> extends Instance {
    /**
     * Returns the name of this multimap.
     *
     * @return the name of this multimap
     */
    String getName();

    /**
     * Stores a key-value pair in the multimap.
     *
     * @param key   the key to be stored
     * @param value the value to be stored
     * @return true if size of the multimap is increased, false if the multimap
     *         already contains the key-value pair.
     */
    boolean put(K key, V value);

    /**
     * Returns the collection of values associated with the key.
     *
     * @param key the key whose associated values are to be returned
     * @return the collection of the values associated with the key.
     */
    Collection<V> get(K key);

    /**
     * Removes the given key value pair from the multimap.
     *
     * @param key   the key of the entry to remove
     * @param value the value of the entry to remove
     * @return true if the size of the multimap changed after the remove operation, false otherwise.
     */
    boolean remove(Object key, Object value);

    /**
     * Removes all the entries with the given key.
     *
     * @param key the key of the entries to remove
     * @return the collection of removed values associated with the given key. Returned collection
     *         might be modifiable but it has no effect on the multimap
     */
    Collection<V> remove(Object key);

    /**
     * Returns the set of keys in the multimap.
     *
     * @return the set of keys in the multimap. Returned set might be modifiable
     *         but it has no effect on the multimap
     */
    Set<K> keySet();

    /**
     * Returns the collection of values in the multimap.
     *
     * @return the collection of values in the multimap. Returned collection might be modifiable
     *         but it has no effect on the multimap
     */
    Collection<V> values();

    /**
     * Returns the set of key-value pairs in the multimap.
     *
     * @return the set of key-value pairs in the multimap. Returned set might be modifiable
     *         but it has no effect on the multimap
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Returns whether the multimap contains an entry with the key.
     *
     * @param key the key whose existence is checked.
     * @return true if the multimap contains an entry with the key, false otherwise.
     */
    boolean containsKey(K key);

    /**
     * Returns whether the multimap contains an entry with the value.
     *
     * @param value the value whose existence is checked.
     * @return true if the multimap contains an entry with the value, false otherwise.
     */
    boolean containsValue(Object value);

    /**
     * Returns whether the multimap contains the given key-value pair.
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
     * Returns number of values matching to given key in the multimap.
     *
     * @param key the key whose values count are to be returned
     * @return number of values matching to given key in the multimap.
     */
    int valueCount(K key);

    /**
     * Adds an entry listener for this multimap. Listener will get notified
     * for all multimap add/remove/update/evict events.
     *
     * @param listener     entry listener
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     */
    void addEntryListener(EntryListener<K, V> listener, boolean includeValue);

    /**
     * Removes the specified entry listener
     * Returns silently if there is no such listener added before.
     *
     * @param listener entry listener
     */
    void removeEntryListener(EntryListener<K, V> listener);

    /**
     * Adds the specified entry listener for the specified key.
     * The listener will get notified for all
     * add/remove/update/evict events of the specified key only.
     *
     * @param listener     entry listener
     * @param key          the key to listen
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     */
    void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue);

    /**
     * Removes the specified entry listener for the specified key.
     * Returns silently if there is no such listener added before for
     * the key.
     *
     * @param listener
     * @param key
     */
    void removeEntryListener(EntryListener<K, V> listener, K key);

    /**
     * Acquires the lock for the specified key.
     * <p>If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     * <p/>
     * Scope of the lock is this multimap only.
     * Acquired lock is only for the key in this multimap.
     * <p/>
     * Locks are re-entrant so if the key is locked N times then
     * it should be unlocked N times before another thread can acquire it.
     *
     * @param key key to lock.
     */
    void lock(K key);

    /**
     * Tries to acquire the lock for the specified key.
     * <p>If the lock is not available then the current thread
     * doesn't wait and returns false immediately.
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
     *
     * @param time     the maximum time to wait for the lock
     * @param timeunit the time unit of the <tt>time</tt> argument.
     * @return <tt>true</tt> if the lock was acquired and <tt>false</tt>
     *         if the waiting time elapsed before the lock was acquired.
     */
    boolean tryLock(K key, long time, TimeUnit timeunit);

    /**
     * Releases the lock for the specified key. It never blocks and
     * returns immediately.
     *
     * @param key key to lock.
     */
    void unlock(K key);
}

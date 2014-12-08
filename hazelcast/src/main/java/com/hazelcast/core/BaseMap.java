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

/**
 * Base interface for Hazelcast distributed maps.
 *
 * @param <K> Key
 * @param <V> Value
 * @see IMap
 * @see TransactionalMap
 */
public interface BaseMap<K, V> extends DistributedObject {

    /**
     * Returns {@code true} if this map contains an entry for the specified
     * key.
     *
     * @param key The specified key.
     * @return {@code true} if this map contains an entry for the specified key.
     */
    boolean containsKey(Object key);

    /**
     * Returns the value for the specified key, or {@code null} if this map does not contain this key.
     *
     * @param key The specified key.
     * @return The value for the specified key.
     */
    V get(Object key);

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.
     *
     * @param key   The specified key.
     * @param value The value to associate with the key.
     * @return Previous value associated with {@code key} or {@code null}
     * if there was no mapping for {@code key}.
     */
    V put(K key, V value);

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.
     * <p/>
     * <p/> This method is preferred to {@link #put(Object, Object)}
     * if the old value is not needed.
     *
     * @param key   The specified key.
     * @param value The value to associate with the key.
     */
    void set(K key, V value);

    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * This is equivalent to
     * <pre>
     *   if (!map.containsKey(key))
     *       return map.put(key, value);
     *   else
     *       return map.get(key);</pre>
     * except that the action is performed atomically.
     *
     * @param key   The specified key.
     * @param value The value to associate with the key when there is no previous value.
     * @return The previous value associated with {@code key}, or {@code null}
     * if there was no mapping for {@code key}.
     */
    V putIfAbsent(K key, V value);

    /**
     * Replaces the entry for a key only if it is currently mapped to some value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key)) {
     *       return map.put(key, value);
     *   } else return null;</pre>
     * except that the action is performed atomically.
     *
     * @param key   The specified key.
     * @param value The value to replace the previous value.
     * @return The previous value associated with {@code key}, or {@code null}
     * if there was no mapping for {@code key}.
     */
    V replace(K key, V value);

    /**
     * Replaces the entry for a key only if currently mapped to a given value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key) &amp;&amp; map.get(key).equals(oldValue)) {
     *       map.put(key, newValue);
     *       return true;
     *   } else return false;</pre>
     * except that the action is performed atomically.
     *
     * @param key      The specified key.
     * @param oldValue Replace the key value if it is the old value.
     * @param newValue The new value to replace the old value.
     * @return {@code true} if the value was replaced.
     */
    boolean replace(K key, V oldValue, V newValue);

    /**
     * Removes the mapping for a key from this map if it is present.
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key Remove the mapping for this key.
     * @return The previous value associated with {@code key}, or {@code null}
     * if there was no mapping for {@code key}.
     */
    V remove(Object key);


    /**
     * Removes the mapping for a key from this map if it is present.
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     * <p/>
     * * <p> This method is preferred to {@link #remove(Object)}
     * if the old value is not needed.
     *
     * @param key Remove the mapping for this key.
     */
    void delete(Object key);

    /**
     * Removes the entry for a key only if currently mapped to a given value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key) &amp;&amp; map.get(key).equals(value)) {
     *       map.remove(key);
     *       return true;
     *   } else return false;</pre>
     * except that the action is performed atomically.
     *
     * @param key   The specified key.
     * @param value Remove the key if it has this value.
     * @return {@code true} if the value was removed.
     */
    boolean remove(Object key, Object value);

    /**
     * Returns <tt>true</tt> if this map contains no entries.
     *
     * @return <tt>true</tt> if this map contains no entries.
     */
    boolean isEmpty();

    /**
     * Returns the number of entries in this map.
     *
     * @return the number of entries in this map.
     */
    int size();
}

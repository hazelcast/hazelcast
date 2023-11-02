/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import com.hazelcast.core.DistributedObject;

/**
 * CPMap is a key-value store within CP. It can be accessed via {@link CPSubsystem#getMap(String)}.
 * @param <K> Key
 * @param <V> Value
 * @since 5.4
 */
public interface CPMap<K, V> extends DistributedObject {
    /**
     * Associates [key] with [value].
     * <p>
     *     See {@link CPMap#set(K, V)} for a more optimal solution when the previous value of [key] is not relevant.
     * </p>
     * @param key non-null key of the entry
     * @param value Value of the entry
     * @return null if [key] had no previous mapping, otherwise the previous value associated with [key]
     * @throws IllegalArgumentException when [key] is null
     */
    V put(K key, V value);

    /**
     * Associates [key] with [value].
     * <p>
     *     This method should be preferred over {@link CPMap#put(K, V)} as it has a smaller network footprint due to the previous
     *     value associated with [key] not being transmitted. Use {@link CPMap#put(K, V)} only when the previous value of [key] is
     *     required.
     * </p>
     * @param key non-null key of the entry
     * @param value Value of the entry
     * @throws IllegalArgumentException when [key] is null
     */
    void set(K key, V value);

    /**
     * Removes [key] if present.
     * @param key non-null key of the key-value entry to remove
     * @return null if [key] was not present, otherwise the value associated with [key]
     * @throws IllegalArgumentException when [key] is null
     */
    V remove(K key);

    /**
     * Removes [key] if present.
     * @param key non-null key of the key-value entry to remove
     * @throws IllegalArgumentException when [key] is null
     */
    void delete(K key);

    /**
     * Indivisibly sets [key] to [newValue] if the current value for [key] is equal-to [expectedValue].
     * @param key non-null key of the entry
     * @param expectedValue Expected value associated with [key]
     * @param newValue New value to associated with [key]
     * @return true if [key] was associated with [newValue], otherwise false
     * @throws IllegalArgumentException when [key] is null
     */
    boolean compareAndSet(K key, V expectedValue, V newValue);

    /**
     * Gets the value associated with [key]
     * @param key non-null key of the entry
     * @return null if [key] had no association, otherwise the value associated with [key].
     * @throws IllegalArgumentException when [key] is null
     */
    V get(K key);
}

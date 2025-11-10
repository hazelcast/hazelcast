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

package com.hazelcast.cp;

import com.hazelcast.core.DistributedObject;

import javax.annotation.Nonnull;

/**
 * CPMap is a key-value store within CP. It can be accessed via {@link CPSubsystem#getMap(String)}.
 * <p>
 *     A CPMap must be able to fit within the member's host RAM. A CPMap is not partitioned like an
 *     {@link com.hazelcast.map.IMap}.
 * </p>
 * @param <K> Key
 * @param <V> Value
 * @since 5.4
 */
public interface CPMap<K, V> extends DistributedObject {
    /**
     * Associates {@code key} with {@code value}.
     * <p>
     *     See {@link CPMap#set(Object, Object)} for a more optimal solution when the previous value of @{code key}
     *     is not relevant.
     * </p>
     * @param key non-null key of the entry
     * @param value non-null value of the entry
     * @return null if {@code key} had no previous mapping, otherwise the previous value associated with {@code key}
     * @throws NullPointerException when {@code key} or {@code value} is null
     */
    V put(@Nonnull K key, @Nonnull V value);

    /**
     * Associates {@code key} with {@code value} only if {@code key} is not present.
     * @param key non-null key of the entry
     * @param value non-null value of the entry
     * @return null if {@code key} had no previous mapping, otherwise the value associated with {@code key}
     * @throws NullPointerException when {@code key} or {@code value} is null
     */
    V putIfAbsent(@Nonnull K key, @Nonnull V value);

    /**
     * Associates {@code key} with {@code value}.
     * <p>
     *     This method should be preferred over {@link CPMap#put(Object, Object)} as it has a smaller network footprint
     *     due to the previous value associated with {@code key} not being transmitted. Use
     *     {@link CPMap#put(Object, Object)} only when the previous value of {@code key} is required.
     * </p>
     * @param key non-null key of the entry
     * @param value non-null value of the entry
     * @throws NullPointerException when {@code key} or {@code value} is null
     */
    void set(@Nonnull K key, @Nonnull V value);

    /**
     * Removes {@code key} if present.
     * @param key non-null key of the key-value entry to remove
     * @return null if {@code key} was not present, otherwise the value associated with {@code key}
     * @throws NullPointerException when {@code key} is null
     */
    V remove(@Nonnull K key);

    /**
     * Removes {@code key} if present.
     * @param key non-null key of the key-value entry to remove
     * @throws NullPointerException when {@code key} is null
     */
    void delete(@Nonnull K key);

    /**
     * Atomically sets {@code key} to {@code newValue} if the current value for {@code key} is equal-to {@code expectedValue}.
     * @param key non-null key of the entry
     * @param expectedValue non-null expected value associated with {@code key}
     * @param newValue non-null new value to associate with {@code key}
     * @return true if {@code key} was associated with {@code newValue}, otherwise false
     * @throws NullPointerException when {@code key}, {@code expectedValue} or {@code newValue} is null
     */
    boolean compareAndSet(@Nonnull K key, @Nonnull V expectedValue, @Nonnull V newValue);

    /**
     * Gets the value associated with {@code key}
     * @param key non-null key of the entry
     * @return null if {@code key} had no association, otherwise the value associated with {@code key}
     * @throws NullPointerException when {@code key} is null
     */
    V get(@Nonnull K key);
}

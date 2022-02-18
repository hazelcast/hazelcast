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

package com.hazelcast.map.impl.querycache;

import java.util.Map;

/**
 * General contract to define any {@code id --&gt; item} registration.
 *
 * @param <K> the type of key for reaching an item from this registry.
 * @param <T> the type of item to register.
 */
public interface Registry<K, T> {

    /**
     * Returns item if it exists in this registry or creates it.
     *
     * @param id key for reaching the item from this registry.
     * @return registered item.
     */
    T getOrCreate(K id);

    /**
     * Returns item if it exists in this registry otherwise returns null.
     *
     * @param id key for reaching the item from this registry.
     * @return registered item or null.
     */
    T getOrNull(K id);

    /**
     * Returns map of all registered items in this registry.
     *
     * @return map of all registered items in this registry
     */
    Map<K, T> getAll();

    /**
     * Removes the registration from this registry.
     *
     * @param id key for reaching the item from this registry.
     * @return removed registry entry.
     */
    T remove(K id);
}

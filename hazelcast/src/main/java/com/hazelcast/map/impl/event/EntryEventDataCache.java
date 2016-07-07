/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.event;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;

/**
 * A cache for {@code EntryEventData}. Instances of this interface are obtained by {@link FilteringStrategy}s. This provides a
 * chance for filtering strategies to optimize event data caching implementation. This is not a general-purpose cache, rather it
 * is meant to be used within the context of processing a single entry event in single-threaded code.
 *
 * @see MapEventPublisherImpl#publishEvent(Collection, Address, String, EntryEventType, Data, Object, Object, Object)
 */
public interface EntryEventDataCache {

    /**
     * If an {@code EntryEventData} for the given parameters is already cached then return the cached value, otherwise create,
     * cache and return the {@code EntryEventData}.
     * @param mapName           name of map
     * @param caller
     * @param dataKey
     * @param newValue
     * @param oldValue
     * @param mergingValue
     * @param eventType
     * @param includingValues
     * @return {@code EntryEventData} already cached in {@code Map eventDataPerEventType} for the given {@code eventType} or
     *          if not already cached, a new {@code EntryEventData} object.
     */
    EntryEventData getOrCreateEventData(String mapName, Address caller, Data dataKey, Object newValue, Object oldValue,
                                        Object mergingValue, int eventType, boolean includingValues);

    /**
     * Indicate whether event data have been created and are cached in this cache or not. If this method returns false, then
     * {@code eventDataIncludingValues.size() + eventDataExcludingValues.size() > 0}.
     * @return {@code true} if event data were not created in this cache, otherwise false.
     */
    boolean isEmpty();

    /**
     * Return {@code EntryEventData} created & cached by invocations to
     * {@link #getOrCreateEventData(String, Address, Data, Object, Object, Object, int, boolean)}, including values in
     * the {@code EntryEventData} object.
     * @return {@code EntryEventData} created & cached including values. When no such {@code EntryEventData} exist, may
     *         return {@code null} or empty collection.
     */
    Collection<EntryEventData> eventDataIncludingValues();

    /**
     * Return {@code EntryEventData} created & cached by invocations to
     * {@link #getOrCreateEventData(String, Address, Data, Object, Object, Object, int, boolean)}, excluding values from
     * the {@code EntryEventData} object.
     * @return {@code EntryEventData} created & cached excluding values. When no such {@code EntryEventData} exist, may
     *         return {@code null} or empty collection.
     */
    Collection<EntryEventData> eventDataExcludingValues();

}

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

package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;

import java.util.Collection;

/**
 * A cache for {@link EntryEventData}. Instances of this interface are obtained by {@link FilteringStrategy}s. This provides a
 * chance for filtering strategies to optimize event data caching implementation. This is not a general-purpose cache, rather it
 * is meant to be used within the context of processing a single entry event in single-threaded code. This will
 * allow creating less instances that are shared between listener registrations. The concrete number of created instances
 * depends on the implementation. Every new event will need to obtain a new instance of the {@link EntryEventDataCache}.
 *
 * @see MapEventPublisherImpl#publishEvent(Collection, Address, String, EntryEventType, Data, Object, Object, Object)
 */
public interface EntryEventDataCache {

    /**
     * If an {@link EntryEventData} for the given parameters is already cached then return the cached value, otherwise create,
     * cache and return the {@link EntryEventData}.
     *
     * @param mapName         name of map
     * @param caller          the address of the caller that caused the event
     * @param dataKey         the key of the event map entry
     * @param newValue        the new value of the map entry
     * @param oldValue        the old value of the map entry
     * @param mergingValue    the value used when performing a merge operation in case of a {@link EntryEventType#MERGED} event.
     *                        This value together with the old value produced the new value.
     * @param eventType       the event type
     * @param includingValues if all of the entry values need to be included in the returned {@link EntryEventData}
     * @return {@link EntryEventData} already cached in {@code Map eventDataPerEventType} for the given {@code eventType} or
     * if not already cached, a new {@link EntryEventData} object.
     */
    EntryEventData getOrCreateEventData(String mapName, Address caller, Data dataKey, Object newValue, Object oldValue,
                                        Object mergingValue, int eventType, boolean includingValues);

    /**
     * Indicate whether event data have been created and are cached in this cache or not. If this method returns false, then
     * {@code eventDataIncludingValues.size() + eventDataExcludingValues.size() > 0}.
     *
     * @return {@code true} if event data were not created in this cache, otherwise false.
     */
    boolean isEmpty();

    /**
     * Return {@link EntryEventData} created &amp; cached by invocations to
     * {@link #getOrCreateEventData(String, Address, Data, Object, Object, Object, int, boolean)}, including values in
     * the {@link EntryEventData} object.
     *
     * @return {@link EntryEventData} created &amp; cached including values. When no such {@link EntryEventData} exist, may
     * return {@code null} or empty collection.
     */
    Collection<EntryEventData> eventDataIncludingValues();

    /**
     * Return {@link EntryEventData} created &amp; cached by invocations to
     * {@link #getOrCreateEventData(String, Address, Data, Object, Object, Object, int, boolean)}, excluding values from
     * the {@link EntryEventData} object.
     *
     * @return {@link EntryEventData} created &amp; cached excluding values. When no such {@link EntryEventData} exist, may
     * return {@code null} or empty collection.
     */
    Collection<EntryEventData> eventDataExcludingValues();

}

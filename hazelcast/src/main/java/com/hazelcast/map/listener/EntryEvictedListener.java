/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.listener;

import com.hazelcast.core.EntryEvent;

/**
 * Invoked upon eviction of an entry.
 *
 * Implementations of this interface receive events after removal of the entry. Removals can be caused by
 * one of size-based-eviction, time-to-live based expiration or max-idle-seconds based expiration.
 *
 * Note that if your listener implements both {@link EntryExpiredListener} and {@link EntryEvictedListener} together,
 * there is a probability that the listener may receive both expiration and eviction events for the same entry. This is because,
 * size-based-eviction removes entries regardless of whether entries expired or not.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 *
 * @see EntryExpiredListener
 *
 * @since 3.5
 */
@FunctionalInterface
public interface EntryEvictedListener<K, V> extends MapListener {

    /**
     * Invoked upon eviction of an entry.
     *
     * @param event the event invoked when an entry is evicted
     */
    void entryEvicted(EntryEvent<K, V> event);
}

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
 * Listener which is notified after removal of an entry due to the expiration-based-eviction.
 * There are two sources of expiration based eviction, they are max-idle-seconds and time-to-live-seconds.
 *
 * Note that if your listener implements both {@link EntryExpiredListener} and {@link EntryEvictedListener} together,
 * there is a probability that the listener may receive both expiration and eviction events for the same entry. This is because,
 * size-based-eviction removes entries regardless of whether entries expired or not.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 *
 * @see EntryEvictedListener
 *
 * @since 3.6
 */
@FunctionalInterface
public interface EntryExpiredListener<K, V> extends MapListener {

    /**
     * Invoked upon expiration of an entry.
     *
     * @param event the event invoked when an entry is expired.
     */
    void entryExpired(EntryEvent<K, V> event);
}

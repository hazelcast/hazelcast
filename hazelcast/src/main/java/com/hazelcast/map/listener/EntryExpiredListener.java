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

package com.hazelcast.map.listener;

import com.hazelcast.core.EntryEvent;

/**
 * Invoked upon expiration-based removal of an entry.
 * <p>
 * Expiration-based entry removals can happen in two different ways:
 * <ul>
 * <li> time-to-live-seconds based expiration</li>
 * <li> max-idle-time based expiration</li>
 * </ul>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
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

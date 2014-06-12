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

import java.util.EventListener;

/**
 * Map Entry listener to get notified when a map entry
 * is added, removed, updated or evicted.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry.
 * @see com.hazelcast.core.IMap#addEntryListener(EntryListener, boolean)
 */
public interface EntryListener<K, V> extends EventListener {

    /**
     * Invoked when an entry is added.
     *
     * @param event entry event
     */
    void entryAdded(EntryEvent<K, V> event);

    /**
     * Invoked when an entry is removed.
     *
     * @param event entry event
     */
    void entryRemoved(EntryEvent<K, V> event);

    /**
     * Invoked when an entry is updated.
     *
     * @param event entry event
     */
    void entryUpdated(EntryEvent<K, V> event);

    /**
     * Invoked when an entry is evicted.
     *
     * @param event entry event
     */
    void entryEvicted(EntryEvent<K, V> event);

    /**
     * Invoked when all entries evicted by {@link IMap#evictAll()}.
     *
     * @param event entry event
     */
    void evictedAll(MapWideEvent event);
}

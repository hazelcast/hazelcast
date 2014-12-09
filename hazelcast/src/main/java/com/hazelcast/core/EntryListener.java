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
 * is added, removed, updated or evicted.  Events will fire as a result
 * of operations carried out via the {@link com.hazelcast.core.IMap} interface.  Events will not fire, for example,
 * for an entry that comes into the Map via the {@link MapLoader} lifecycle.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry.
 * @see com.hazelcast.core.IMap#addEntryListener(EntryListener, boolean)
 */
public interface EntryListener<K, V> extends EventListener {

    /**
     * Invoked when an entry is added.
     *
     * @param event the event invoked when an entry is added
     */
    void entryAdded(EntryEvent<K, V> event);

    /**
     * Invoked when an entry is removed.
     *
     * @param event the event invoked when an entry is removed
     */
    void entryRemoved(EntryEvent<K, V> event);

    /**
     * Invoked when an entry is updated.
     *
     * @param event the event invoked when an entry is updated
     */
    void entryUpdated(EntryEvent<K, V> event);

    /**
     * Invoked when an entry is evicted.
     *
     * @param event the event invoked when an entry is evicted
     */
    void entryEvicted(EntryEvent<K, V> event);

    /**
     * Invoked when all entries are evicted by {@link IMap#evictAll()}.
     *
     * @param event the map event invoked when all entries are evicted by {@link IMap#evictAll()}
     */
    void mapEvicted(MapEvent event);

    /**
     * Invoked when all entries are removed by {@link IMap#clear()}.
     *
     * @param event the map event invoked when all entries are removed by {@link IMap#clear()}

     */
    void mapCleared(MapEvent event);
}

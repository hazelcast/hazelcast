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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.hazelcast.map.listener.MapEvictedListener;

/**
 * Internal usage only adapter for {@link
 * com.hazelcast.map.listener.MapListener}.
 *
 * The difference between this adapter and {@link EntryAdapter} is,
 * {@link EntryAdapter} is more limited form of this one and it doesn't
 * implement newly added listener interfaces.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry.
 * @see com.hazelcast.map.listener.MapListener
 * @since 3.6
 */

public class MapListenerAdapter<K, V> implements EntryAddedListener<K, V>,
        EntryUpdatedListener<K, V>, EntryRemovedListener<K, V>,
        EntryEvictedListener<K, V>, EntryExpiredListener<K, V>,
        EntryMergedListener<K, V>, EntryLoadedListener<K, V>,
        MapClearedListener, MapEvictedListener {

    @Override
    public void entryAdded(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void entryRemoved(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void entryUpdated(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void entryEvicted(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void entryExpired(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void entryMerged(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void entryLoaded(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }

    @Override
    public void mapEvicted(MapEvent event) {
        onMapEvent(event);
    }

    @Override
    public void mapCleared(MapEvent event) {
        onMapEvent(event);
    }

    /**
     * This method is called when an one of the methods of the {@link com.hazelcast.core.EntryListener} is not
     * overridden. It can be practical if you want to bundle some/all of the methods to a single method.
     *
     * @param event the EntryEvent.
     */
    public void onEntryEvent(EntryEvent<K, V> event) {
    }

    /**
     * This method is called when an one of the methods of the {@link com.hazelcast.core.EntryListener} is not
     * overridden. It can be practical if you want to bundle some/all of the methods to a single method.
     *
     * @param event the MapEvent.
     */
    public void onMapEvent(MapEvent event) {
    }
}

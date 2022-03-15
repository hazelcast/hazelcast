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

package com.hazelcast.core;

import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.hazelcast.map.listener.MapEvictedListener;

/**
 * Map Entry listener to get notified when a map entry is added,
 * removed, updated, evicted or expired.  Events will fire as a result
 * of operations carried out via the {@link IMap}
 * interface.  Events will not fire, for example, for an entry
 * that comes into the Map via the {@link MapLoader} lifecycle.
 * <p>
 * This interface is here for backward compatibility reasons.
 * For a most appropriate alternative please use/check
 * {@link com.hazelcast.map.listener.MapListener} interface.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see IMap#addEntryListener
 * @see com.hazelcast.map.listener.MapListener
 */
public interface EntryListener<K, V>
        extends EntryAddedListener<K, V>,
        EntryUpdatedListener<K, V>,
        EntryRemovedListener<K, V>,
        EntryEvictedListener<K, V>,
        EntryExpiredListener<K, V>,
        MapClearedListener,
        MapEvictedListener {

}

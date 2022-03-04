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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindProcessor;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writethrough.WriteThroughStore;

/**
 * Factory class responsible for creating
 * various data store implementations.
 *
 * @see com.hazelcast.map.impl.mapstore.MapDataStore
 */
public final class MapDataStores {

    public static final MapDataStore EMPTY_MAP_DATA_STORE = new EmptyMapDataStore();

    private MapDataStores() {
    }

    /**
     * Creates a write behind data store.
     *
     * @param mapStoreContext      context for map store operations
     * @param partitionId          partition ID of partition
     * @param writeBehindProcessor the {@link WriteBehindProcessor}
     * @param <K>                  type of key to store
     * @param <V>                  type of value to store
     * @return new write behind store manager
     */
    public static <K, V> MapDataStore<K, V> createWriteBehindStore(MapStoreContext mapStoreContext,
                                                                   int partitionId,
                                                                   WriteBehindProcessor writeBehindProcessor) {
        return (MapDataStore<K, V>) new WriteBehindStore(mapStoreContext, partitionId, writeBehindProcessor);
    }

    /**
     * Creates a write through data store.
     *
     * @param mapStoreContext context for map store operations
     * @param <K>             type of key to store
     * @param <V>             type of value to store
     * @return new write through store manager
     */
    public static <K, V> MapDataStore<K, V> createWriteThroughStore(MapStoreContext mapStoreContext) {
        return (MapDataStore<K, V>) new WriteThroughStore(mapStoreContext);

    }
}

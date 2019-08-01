/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindProcessor;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writethrough.WriteThroughStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createBoundedWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;

/**
 * Factory class responsible for creating various data store implementations.
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
    public static <K, V> MapDataStore<K, V> createWriteBehindStore(MapStoreContext mapStoreContext, int partitionId,
                                                                   WriteBehindProcessor writeBehindProcessor) {
        MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        InternalSerializationService serializationService
                = ((InternalSerializationService) nodeEngine.getSerializationService());
        WriteBehindStore mapDataStore = new WriteBehindStore(mapStoreContext, partitionId, serializationService);
        mapDataStore.setWriteBehindQueue(newWriteBehindQueue(mapServiceContext, mapStoreConfig.isWriteCoalescing()));
        mapDataStore.setWriteBehindProcessor(writeBehindProcessor);
        return (MapDataStore<K, V>) mapDataStore;
    }

    private static WriteBehindQueue newWriteBehindQueue(MapServiceContext mapServiceContext, boolean writeCoalescing) {
        HazelcastProperties hazelcastProperties = mapServiceContext.getNodeEngine().getProperties();
        final int capacity = hazelcastProperties.getInteger(GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY);
        final AtomicInteger counter = mapServiceContext.getWriteBehindQueueItemCounter();
        return (writeCoalescing ? createDefaultWriteBehindQueue() : createBoundedWriteBehindQueue(capacity, counter));
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
        final MapStoreWrapper store = mapStoreContext.getMapStoreWrapper();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalSerializationService serializationService
                = ((InternalSerializationService) nodeEngine.getSerializationService());

        return (MapDataStore<K, V>) new WriteThroughStore(store, serializationService);

    }

    /**
     * Used for providing neutral null behaviour.
     *
     * @param <K> type of key to store
     * @param <V> type of value to store
     * @return empty store manager
     */
    public static <K, V> MapDataStore<K, V> emptyStore() {
        return (MapDataStore<K, V>) EMPTY_MAP_DATA_STORE;
    }
}

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindProcessor;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writethrough.WriteThroughStore;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createSafeBoundedArrayWriteBehindQueue;

/**
 * Factory class responsible for creating various data store implementations.
 *
 * @see com.hazelcast.map.impl.mapstore.MapDataStore
 */
public final class MapDataStores {

    private MapDataStores() {
    }

    /**
     * Creates a write behind data store.
     *
     * @param mapStoreContext      context for map store operations.
     * @param partitionId          partition id of partition.
     * @param writeBehindProcessor the {@link WriteBehindProcessor}
     * @param <K>                  type of key to store.
     * @param <V>                  type of value to store.
     * @return new write behind store manager.
     */
    public static <K, V> MapDataStore<K, V> createWriteBehindStore(MapStoreContext mapStoreContext, int partitionId,
                                                                   WriteBehindProcessor writeBehindProcessor) {
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        final MapStoreWrapper store = mapStoreContext.getMapStoreWrapper();
        final SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        final MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        final int writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();
        final long writeDelayMillis = TimeUnit.SECONDS.toMillis(writeDelaySeconds);
        final boolean writeCoalescing = mapStoreConfig.isWriteCoalescing();
        final WriteBehindStore mapDataStore
                = new WriteBehindStore(store, serializationService, writeDelayMillis, partitionId);
        final WriteBehindQueue writeBehindQueue = newWriteBehindQueue(mapServiceContext, writeCoalescing);
        mapDataStore.setWriteBehindQueue(writeBehindQueue);
        mapDataStore.setWriteBehindProcessor(writeBehindProcessor);
        return (MapDataStore<K, V>) mapDataStore;
    }

    private static WriteBehindQueue newWriteBehindQueue(MapServiceContext mapServiceContext, boolean writeCoalescing) {
        final int capacity = mapServiceContext.getNodeEngine().getGroupProperties().MAP_WRITE_BEHIND_QUEUE_CAPACITY.getInteger();
        final AtomicInteger counter = mapServiceContext.getWriteBehindQueueItemCounter();
        return writeCoalescing ? createDefaultWriteBehindQueue()
                : createSafeBoundedArrayWriteBehindQueue(capacity, counter);
    }

    /**
     * Creates a write through data store.
     *
     * @param mapStoreContext context for map store operations.
     * @param <K>             type of key to store.
     * @param <V>             type of value to store.
     * @return new write through store manager.
     */
    public static <K, V> MapDataStore<K, V> createWriteThroughStore(MapStoreContext mapStoreContext) {
        final MapStoreWrapper store = mapStoreContext.getMapStoreWrapper();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final SerializationService serializationService = nodeEngine.getSerializationService();

        return (MapDataStore<K, V>) new WriteThroughStore(store, serializationService);

    }

    /**
     * Used for providing neutral null behaviour.
     *
     * @param <K> type of key to store.
     * @param <V> type of value to store.
     * @return empty store manager.
     */
    public static <K, V> MapDataStore<K, V> emptyStore() {
        return (MapDataStore<K, V>) EmptyStoreHolder.EMPTY;
    }

    private static class EmptyStoreHolder {
        static final MapDataStore EMPTY = new EmptyMapDataStore();
    }
}

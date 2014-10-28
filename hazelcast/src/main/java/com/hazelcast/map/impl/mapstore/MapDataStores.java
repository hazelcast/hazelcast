package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindProcessor;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writethrough.WriteThroughStore;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createSafeBoundedArrayWriteBehindQueue;

/**
 * Factory class responsible for creating various daa store implementations.
 *
 * @see com.hazelcast.map.impl.mapstore.MapDataStore
 */
public final class MapDataStores {

    private MapDataStores() {
    }

    /**
     * Creates a write behind data store.
     *
     * @param mapContainer corresponding container of map.
     * @param partitionId  partition id of partition.
     * @param <K>          type of key to store.
     * @param <V>          type of value to store.
     * @return new write behind store manager.
     */
    public static <K, V> MapDataStore<K, V> createWriteBehindStore(MapContainer mapContainer, int partitionId,
                                                                   WriteBehindProcessor writeBehindProcessor) {
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final MapStoreWrapper store = mapContainer.getStore();
        final SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        final int writeDelaySeconds = mapContainer.getMapConfig().getMapStoreConfig().getWriteDelaySeconds();
        final long writeDelayMillis = TimeUnit.SECONDS.toMillis(writeDelaySeconds);
        // TODO writeCoalescing should be configurable.
        boolean writeCoalescing = true;
        final WriteBehindStore mapDataStore
                = new WriteBehindStore(store, serializationService, writeDelayMillis, partitionId, writeCoalescing);
        final WriteBehindQueue writeBehindQueue = pickWriteBehindQueue(mapServiceContext, writeCoalescing);
        mapDataStore.setWriteBehindQueue(writeBehindQueue);
        mapDataStore.setWriteBehindProcessor(writeBehindProcessor);
        return (MapDataStore<K, V>) mapDataStore;
    }

    private static WriteBehindQueue pickWriteBehindQueue(MapServiceContext mapServiceContext, boolean writeCoalescing) {
        final int capacity = mapServiceContext.getNodeEngine().getGroupProperties().MAP_WRITE_BEHIND_QUEUE_CAPACITY.getInteger();
        final AtomicInteger counter = mapServiceContext.getWriteBehindQueueItemCounter();
        return writeCoalescing ? createDefaultWriteBehindQueue()
                : createSafeBoundedArrayWriteBehindQueue(capacity, counter);
    }

    /**
     * Creates a write through data store.
     *
     * @param mapContainer corresponding container of map.
     * @param <K>          type of key to store.
     * @param <V>          type of value to store.
     * @return new write through store manager.
     */
    public static <K, V> MapDataStore<K, V> createWriteThroughStore(MapContainer mapContainer) {
        return (MapDataStore<K, V>) new WriteThroughStore(mapContainer.getStore(),
                mapContainer.getMapServiceContext().getNodeEngine().getSerializationService());
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

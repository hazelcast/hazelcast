package com.hazelcast.map.mapstore;

import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.mapstore.writebehind.WriteBehindProcessor;
import com.hazelcast.map.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.mapstore.writethrough.WriteThroughStore;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory class responsible for creating various daa store implementations.
 *
 * @see com.hazelcast.map.mapstore.MapDataStore
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
        final long millis = mapServiceContext.convertTime(writeDelaySeconds, TimeUnit.SECONDS);
        final int capacity = mapServiceContext.getNodeEngine().getGroupProperties().MAP_WRITE_BEHIND_QUEUE_CAPACITY.getInteger();
        final AtomicInteger writeBehindQueueItemCounter = mapServiceContext.getWriteBehindQueueItemCounter();
        final WriteBehindStore mapDataStore
                = new WriteBehindStore(store, serializationService, millis,
                partitionId, capacity, writeBehindQueueItemCounter);
        mapDataStore.setWriteBehindProcessor(writeBehindProcessor);
        return (MapDataStore<K, V>) mapDataStore;
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

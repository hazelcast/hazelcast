package com.hazelcast.map.impl.mapstore.writethrough;

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapDataStores;
import com.hazelcast.map.impl.mapstore.MapStoreManager;

/**
 * Write through map store manager.
 */
public class WriteThroughManager implements MapStoreManager {

    private final MapDataStore mapDataStore;

    public WriteThroughManager(MapContainer mapContainer) {
        mapDataStore = MapDataStores.createWriteThroughStore(mapContainer);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public MapDataStore getMapDataStore(int partitionId) {
        return mapDataStore;
    }
}

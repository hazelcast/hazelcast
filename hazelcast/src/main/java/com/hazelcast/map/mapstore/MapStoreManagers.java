package com.hazelcast.map.mapstore;

import com.hazelcast.map.MapContainer;
import com.hazelcast.map.mapstore.writebehind.WriteBehindManager;
import com.hazelcast.map.mapstore.writethrough.WriteThroughManager;

/**
 * Static factory class for various map store managers.
 */
public final class MapStoreManagers {
    private MapStoreManagers() {
    }

    public static MapStoreManager createWriteThroughManager(MapContainer mapContainer) {
        return new WriteThroughManager(mapContainer);
    }

    public static MapStoreManager createWriteBehindManager(MapContainer mapContainer) {
        return new WriteBehindManager(mapContainer);
    }

    public static MapStoreManager emptyMapStoreManager() {
        return EmptyHolder.EMPTY;
    }

    private static class EmptyHolder {
        static final MapStoreManager EMPTY = createEmptyManager();
    }

    private static MapStoreManager createEmptyManager() {
        return new MapStoreManager() {
            @Override
            public void start() {

            }

            @Override
            public void stop() {

            }

            @Override
            public MapDataStore getMapDataStore(int partitionId) {
                return MapDataStores.emptyStore();
            }
        };
    }

}

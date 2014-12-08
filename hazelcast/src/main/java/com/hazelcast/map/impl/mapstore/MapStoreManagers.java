package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindManager;
import com.hazelcast.map.impl.mapstore.writethrough.WriteThroughManager;

/**
 * Static factory class for various map store managers.
 */
public final class MapStoreManagers {
    private MapStoreManagers() {
    }

    public static MapStoreManager createWriteThroughManager(MapStoreContext mapStoreContext) {
        return new WriteThroughManager(mapStoreContext);
    }

    public static MapStoreManager createWriteBehindManager(MapStoreContext mapStoreContext) {
        return new WriteBehindManager(mapStoreContext);
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

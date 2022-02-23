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
            public MapDataStore getMapDataStore(String mapName, int partitionId) {
                return MapDataStores.EMPTY_MAP_DATA_STORE;
            }
        };
    }
}

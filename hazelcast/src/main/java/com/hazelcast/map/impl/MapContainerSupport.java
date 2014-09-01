package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;

/**
 * Contains support methods of a map container.
 *
 * @see MapContainer
 */
public abstract class MapContainerSupport {

    protected volatile MapConfig mapConfig;

    protected MapContainerSupport(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    public boolean isMapStoreEnabled() {
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (mapStoreConfig == null || !mapStoreConfig.isEnabled()) {
            return false;
        }
        return true;
    }


    public boolean isWriteBehindMapStoreEnabled() {
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        return mapStoreConfig != null && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }
}

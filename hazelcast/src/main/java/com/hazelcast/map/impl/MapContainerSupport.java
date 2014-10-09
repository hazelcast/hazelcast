package com.hazelcast.map.impl;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;

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

    /**
     * Get max size per node setting form config
     *
     * @return max size or -1 if policy is not set
     **/
    public int getMaxSizePerNode() {
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        int maxSize = -1;

        if (maxSizeConfig.getMaxSizePolicy() == PER_NODE) {
            maxSize = maxSizeConfig.getSize();
        }

        return maxSize;
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }
}

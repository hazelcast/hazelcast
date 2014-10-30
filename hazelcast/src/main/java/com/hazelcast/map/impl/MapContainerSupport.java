package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateTTLMillis;

/**
 * Contains support methods of a map container.
 *
 * @see MapContainer
 */
abstract class MapContainerSupport {

    protected volatile MapConfig mapConfig;

    private final long maxIdleMillis;

    private final long ttlMillisFromConfig;

    private final String name;

    protected MapContainerSupport(String name, MapConfig mapConfig) {
        this.name = name;
        this.mapConfig = mapConfig;
        this.maxIdleMillis = calculateMaxIdleMillis(mapConfig);
        this.ttlMillisFromConfig = calculateTTLMillis(mapConfig);
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
     */
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

    public long getMaxIdleMillis() {
        return maxIdleMillis;
    }

    public long getTtlMillisFromConfig() {
        return ttlMillisFromConfig;
    }

    public String getName() {
        return name;
    }
}

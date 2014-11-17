package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;

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

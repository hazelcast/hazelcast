package com.hazelcast.map.impl.mapstore;

/**
 * Central interface to manage map store.
 */
public interface MapStoreManager {

    void start();

    void stop();

    MapDataStore getMapDataStore(int partitionId);
}

package com.hazelcast.map.mapstore;

/**
 * Central interface to manage map store.
 */
public interface MapStoreManager {

    void start();

    void stop();

    MapDataStore getMapDataStore(int partitionId);
}

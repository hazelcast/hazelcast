package com.hazelcast.map.impl.mapstore;

import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Map;

/**
 * Map data stores general contract.
 * Provides an extra abstraction layer over write-through and write-behind map-store implementations.
 *
 * @param <K> type of key to store.
 * @param <V> type of value to store.
 */
public interface MapDataStore<K, V> {

    V add(K key, V value, long now);

    void addTransient(K key, long now);

    V addBackup(K key, V value, long now);

    void remove(K key, long now);

    void removeBackup(K key, long now);

    /**
     * Clears resources of this map-data-store.
     */
    void clear();

    V load(K key);

    Map loadAll(Collection keys);

    /**
     * Removes keys from map store.
     * It also handles {@link com.hazelcast.nio.serialization.Data} to object conversions of keys.
     *
     * @param keys to be removed.
     */
    void removeAll(Collection keys);

    boolean loadable(K key, long lastUpdateTime, long now);

    int notFinishedOperationsCount();

    boolean isPostProcessingMapStore();

    /**
     * Flushes all keys in this map-store.
     *
     * @return flushed {@link com.hazelcast.nio.serialization.Data} keys list.
     */
    Collection<Data> flush();

    /**
     * Flushes the supplied key to the map-store.
     *
     * @param key    key to be flushed
     * @param value  value to be flushed
     * @param now    now in millis
     * @param backup <code>true</code> calling this method for backup partition, <code>false</code> for owner partition.
     * @return flushed value.
     */
    V flush(K key, V value, long now, boolean backup);
}

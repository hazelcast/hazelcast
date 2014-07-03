package com.hazelcast.map.mapstore;

import java.util.Collection;
import java.util.Map;

/**
 * Map data store general contract.
 *
 * @param <K> type of key to store.
 * @param <V> type of value to store.
 */
public interface MapDataStore<K, V> {

    V add(K key, V value, long now);

    void addTransient(K key, long now);

    V addStagingArea(K key, V value, long now);

    V addBackup(K key, V value, long now);

    void remove(K key, long now);

    void removeBackup(K key, long now);

    void reset();

    V load(K key);

    Map loadAll(Collection keys);

    void removeAll(Collection keys, long now);

    boolean loadable(K key, long lastUpdateTime, long now);

    int notFinishedOperationsCount();

    Collection flush();
}

/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

    boolean loadable(K key);

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

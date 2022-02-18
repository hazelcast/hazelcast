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

package com.hazelcast.map;

import java.util.Collection;
import java.util.Map;

/**
 * Hazelcast distributed map implementation is an in-memory data store, but
 * it can be backed by any type of data store such as RDBMS, OODBMS, NOSQL,
 * or simply a file-based data store.
 * <p>
 * IMap.put(key, value) normally stores the entry into JVM's memory. If the
 * MapStore implementation is provided then Hazelcast will also call the
 * MapStore implementation to store the entry into a user-defined storage, such
 * as RDBMS or some other external storage system. It is completely up to the
 * user how the key-value will be stored or deleted.
 * <p>
 * Same goes for IMap.remove(key).
 * <p>
 * Store implementation can be called synchronously (write-through) or
 * asynchronously (write-behind) depending on the configuration.
 *
 * @param <K> type of the MapStore key
 * @param <V> type of the MapStore value
 */
public interface MapStore<K, V> extends MapLoader<K, V> {

    /**
     * Stores the key-value pair.
     * <p>
     * If an exception is thrown then the put operation will fail.
     *
     * @param key   key of the entry to store
     * @param value value of the entry to store
     */
    void store(K key, V value);

    /**
     * Stores multiple entries.
     * <p>
     * Implementation of this method can optimize the store operation by
     * storing all entries in one database connection. storeAll() is used when
     * writeDelaySeconds is positive (write-behind).If an exception is thrown,
     * the entries will try to be stored one by one using the store() method.
     * <p>
     * Note: on the retry phase only entries left in the map will be stored
     * one-by-one. In this way a MapStore implementation can handle partial
     * storeAll() cases when some entries were stored successfully before a
     * failure happens. Entries removed from the map will be not passed to
     * subsequent call to store() method any more.
     *
     * @param map map of entries to store
     */
    void storeAll(Map<K, V> map);

    /**
     * Deletes the entry with a given key from the store.
     * <p>
     * If an exception is thrown the remove operation will fail.
     *
     * @param key the key to delete from the store
     */
    void delete(K key);

    /**
     * Deletes multiple entries from the store.
     * <p>
     * If an exception is thrown the entries will try to be deleted one by one
     * using the delete() method.
     * <p>
     * Note: on the retry phase only entries left in the keys will be deleted
     * one-by-one. In this way a MapStore implementation can handle partial
     * deleteAll() cases when some entries were deleted successfully before a
     * failure happens. Entries removed from the keys will be not passed to
     * subsequent call to delete() method any more. The intended usage is to
     * delete items from the provided collection as they are deleted from
     * the store.
     *
     * @param keys the keys of the entries to delete.
     */
    void deleteAll(Collection<K> keys);
}

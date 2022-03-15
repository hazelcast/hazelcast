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

package com.hazelcast.collection;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * QueueStore makes a queue backed by a central data store; such as database, disk, etc.
 *
 * @param <T> queue item type
 */
public interface QueueStore<T> {
    /**
     * Stores the key-value pair.
     *
     * @param key   key of the entry to store
     * @param value value of the entry to store
     */
    void store(Long key, T value);

    /**
     * Stores multiple entries. Implementation of this method can optimize the
     * store operation by storing all entries in one database connection for instance.
     *
     * @param map map of entries to store
     */
    void storeAll(Map<Long, T> map);

    /**
     * Deletes the entry with a given key from the store.
     *
     * @param key key to delete from the store.
     */
    void delete(Long key);

    /**
     * Deletes multiple entries from the store.
     *
     * @param keys keys of the entries to delete.
     */
    void deleteAll(Collection<Long> keys);

    /**
     * Loads the value of a given key.
     * <p>
     * Implementation can use any means of loading the given key;
     * such as an O/R mapping tool, simple SQL, reading a file, etc.
     *
     * @param key the key of the requested value
     * @return value of the key
     */
    T load(Long key);

    /**
     * Loads the given keys. This is a batch load operation so that implementation can
     * optimize the multiple loads.
     * <p>
     * Set the bulk-load property to configure batch loading. When the queue is initialized, items are loaded from
     * QueueStore in bulks. Bulk load is the size of these bulks. The default value of bulk-load is 250.
     *
     * @param keys keys of the value entries to load
     * @return map of loaded key-value pairs. May return null if no values loaded.
     */
    Map<Long, T> loadAll(Collection<Long> keys);

    /**
     * Loads all of the keys from the store.
     * <p>
     * The items identified by the keys will be loaded in the iteration order of the returned Set
     *
     * @return all the keys from the store. May return null if no keys loaded.
     */
    Set<Long> loadAllKeys();
}

/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MapStore;

/**
 * Hazelcast distributed map implementation is an in-memory data store
 * but it can be backed by any type of data store such as RDBMS, OODBMS,
 * NOSQL, or simply a file-based data store.
 * <p>
 * IMap.put(key, value) normally stores the entry into JVM's memory. If
 * the EntryStore implementation is provided then Hazelcast will also
 * call theEntryStore implementation to store the entry into a
 * user-defined storage, such as RDBMS or some other external storage
 * system. It is completely up to the user how the key-value will be
 * stored or deleted.
 * <p>
 * Same goes for IMap.remove(key).
 * <p>
 * Store implementation can be called synchronously (write-through) or
 * asynchronously (write-behind) depending on the configuration.
 *
 * @param <K> type of the EntryStore key
 * @param <V> type of the EntryStore value
 */
public interface EntryStore<K, V> extends EntryLoader<K, V>, MapStore<K, EntryLoaderEntry<V>> {
}

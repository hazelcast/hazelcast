/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.annotation.NamespacesSupported;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Hazelcast distributed map implementation is an in-memory data store, but
 * it can be backed by any type of data store such as RDBMS, OODBMS, or simply
 * a file-based data store.
 * <p>
 * {@link IMap#get(Object)} normally returns the value that
 * is available in memory. If the entry doesn't exist in memory, Hazelcast
 * returns {@code null}. If a MapLoader implementation is provided, then instead
 * of returning {@code null}, Hazelcast will attempt to load the unknown entry
 * by calling the implementation's {@link #load(Object)} or
 * {@link #loadAll(Collection)} methods. Loaded entries will be placed into
 * the distributed map, and they will stay in memory until they are explicitly
 * removed or implicitly evicted (if eviction is configured).
 * <p>
 * Do not block a MapLoader with calls that may never return, as Hazelcast
 * does not time out the call. A MapLoader should only load data for its own
 * map; calling a map that has a MapLoader or MapStore from a MapLoader
 * (including its own map) can deadlock.
 * <p>
 * The initial load and the {@code IMap.loadAll} methods always call
 * {@link #loadAll(Collection)} outside partition threads. For other map
 * operations, the thread that calls the MapLoader depends on
 * {@link com.hazelcast.config.MapStoreConfig#setOffload(boolean)}:
 * <ul>
 * <li>With offload enabled (the default), the MapLoader runs on a thread pool
 * shared by all maps. Most other operations on the same partition of the map
 * wait for it, and a slow MapLoader can delay MapStore calls of other maps by
 * occupying all available threads. A blocking call to a map with an
 * offloaded MapStore, such as a get of a key not in memory, waits for a
 * thread of the same pool; a call to the same partition of the MapLoader's
 * own map never runs. Offloaded MapStore operations never time out, so once
 * every thread in the pool is blocked this way, the pool deadlocks
 * permanently.
 * <li>With offload disabled, the MapLoader runs on a partition thread, which
 * meanwhile cannot serve other partitions or data structures handled by that
 * thread, such as another map or a queue, so avoid calling across a WAN or
 * to a system that takes more than a few milliseconds to respond. A
 * synchronous call to a different partition throws
 * {@link java.lang.IllegalThreadStateException}. A Hazelcast client calling a
 * blocked partition thread blocks indefinitely; a call from another member
 * eventually fails with {@link OperationTimeoutException}.
 * </ul>
 *
 * @param <K> type of the MapLoader key
 * @param <V> type of the MapLoader value
 */
@NamespacesSupported
public interface MapLoader<K, V> {

    /**
     * Loads the value of a given key. If distributed map doesn't contain the value
     * for the given key then Hazelcast will call implementation's load (key) method
     * to obtain the value. Implementation can use any means of loading the given key;
     * such as an O/R mapping tool, simple SQL or reading a file etc.
     *
     * @param key cannot be {@code null}
     * @return value of the key; returning {@code null} value signals value missing in the underlying store
     */
    V load(K key);

    /**
     * Loads given keys. This is batch load operation so that implementation can
     * optimize the multiple loads.
     * <p>
     * For any key in the input keys, there should be a single mapping in the resulting map. Also, the resulting
     * map should not have any keys that are not part of the input keys.
     * <p>
     * The given collection should not contain any {@code null} keys.
     * The returned Map should not contain any {@code null} keys or values.
     * <p>
     * Loading other items than what provided in <code>keys</code>
     * prevents the map from being filled from the map store.
     *
     * @param keys keys of the values entries to load
     * @return map of loaded key-value pairs.
     */
    Map<K, V> loadAll(Collection<K> keys);

    /**
     * Loads all the keys from the store. The returned {@link Iterable} may return the keys lazily
     * by loading them in batches. The {@link Iterator} of this {@link Iterable} may implement the
     * {@link Closeable} interface in which case it will be closed once iteration is over.
     * This is intended for releasing resources such as closing a JDBC result set.
     * <p>
     * The returned Iterable should not contain any {@code null} keys.
     *
     * @return all the keys. Keys inside the Iterable cannot be {@code null}.
     */
    Iterable<K> loadAllKeys();
}

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

import com.hazelcast.spi.annotation.NamespacesSupported;

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
 * Implementations <b>must assume</b>:
 * <ul>
 *   <li>Operations may be retried, reordered, or replayed after failures or migrations.</li>
 *   <li>Batch methods ({@code storeAll}/{@code deleteAll}) may partially succeed and be retried
 *       as per-entry operations.</li>
 *   <li>At-least-once delivery semantics; exactly-once or atomic multi-row guarantees are not provided.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Implementations <b>must therefore ensure</b>:
 * <ul>
 *   <li>Idempotent writes and deletes.</li>
 *   <li>Tolerance to partial failure and replay.</li>
 *   <li>No reliance on global ordering, transactional isolation, or side effects.</li>
 * </ul>
 * </p>
 *
 * <p>
 * {@code MapStore} is not a durability layer in the database sense.
 * It exists to make in-memory state recoverable, not to provide transactional persistence.
 * </p>
 * *
 * @param <K> type of the MapStore key
 * @param <V> type of the MapStore value
 */
@NamespacesSupported
public interface MapStore<K, V> extends MapLoader<K, V> {

    /**
     * Stores the key-value pair in the external store.
     * <p>
     * Invocation semantics depend on configuration:
     * <ul>
     *   <li><b>Write-through</b> ({@code writeDelaySeconds == 0}): this method is
     *       invoked synchronously as part of the {@code IMap.put()} call. If this
     *       method throws an exception, the map operation fails.</li>
     *   <li><b>Write-behind</b> ({@code writeDelaySeconds > 0}): this method may be
     *       invoked asynchronously on a background thread, potentially long after
     *       the original map operation has completed.</li>
     * </ul>
     * <p>
     * Under normal circumstances Hazelcast provides <b>at-least-once</b> delivery
     * semantics for calls to this method. Implementations must therefore be
     * idempotent and tolerate retries, reordering, and duplicate invocations.
     *
     * @param key   key of the entry to store
     * @param value value of the entry to store
     */
    void store(K key, V value);

    /**
     * Stores multiple entries in the external store.
     * <p>
     * This method is primarily used with write-behind enabled
     * ({@code writeDelaySeconds > 0}) when batching is applicable. Hazelcast may
     * still invoke {@link #store(Object, Object)} instead of this method depending
     * on runtime conditions (for example, batch size, write coalescing, or number
     * of distinct keys in the current write window). Implementations must not rely
     * on this method being invoked for every flush.
     * <p>
     * <b>Error handling and retries:</b>
     * <ul>
     *   <li>If this method throws an exception, Hazelcast treats the batch as failed and starts retry handling.</li>
     *   <li>If the provided {@code map} has not been modified (no entries removed to signal partial success),
     *       Hazelcast retries {@code storeAll(map)} up to three times, waiting about 1 second between attempts.</li>
     *   <li>After the final batch attempt fails, Hazelcast falls back to calling
     *       {@link #store(Object, Object)} for the remaining entries, one by one.</li>
     *   <li>If the implementation removes entries from {@code map} as they are stored (before throwing),
     *       Hazelcast treats removed entries as successful and does not retry them; only the entries still
     *       present in {@code map} are retried and/or passed to the per-entry fallback.</li>
     * </ul>
     * <p>
     * This allows implementations to signal <b>partial success</b> explicitly.
     * Entries left in the {@code map} after an exception will be retried
     * individually; removed entries will not be passed to subsequent calls.
     * <p>
     * Hazelcast does not provide transactional guarantees across entries. Batch
     * boundaries do not imply atomicity.
     *
     * @param map map of entries to store
     */
    void storeAll(Map<K, V> map);

    /**
     * Deletes the entry with the given key from the external store.
     * <p>
     * Invocation semantics depend on configuration:
     * <ul>
     *   <li><b>Write-through</b>: invoked synchronously as part of
     *       {@code IMap.remove()}.</li>
     *   <li><b>Write-behind</b>: invoked asynchronously on a background thread.</li>
     * </ul>
     * <p>
     * Hazelcast provides <b>at-least-once</b> delivery semantics. Implementations
     * must tolerate retries and duplicate invocations. Deleting a non-existent
     * entry should be treated as a successful no-op.
     *
     * @param key the key to delete from the store
     */
    void delete(K key);

    /**
     * Deletes multiple entries from the external store.
     * <p>
     * This method is primarily used with write-behind enabled
     * ({@code writeDelaySeconds > 0}) when batching is applicable. Hazelcast may
     * still invoke {@link #delete(Object)} instead of this method depending on
     * configuration and runtime conditions. Implementations must not rely on this
     * method being invoked for every flush.
     * <p>
     * <b>Error handling and retries:</b>
     * <ul>
     *   <li>If this method throws an exception, Hazelcast initiates retry handling.</li>
     *   <li>On retry, Hazelcast invokes {@link #delete(Object)} one-by-one for the
     *       remaining keys.</li>
     *   <li>If the implementation removes successfully deleted keys from the
     *       provided {@code keys} collection before throwing, those keys are treated
     *       as successful and are not retried.</li>
     * </ul>
     * <p>
     * This allows implementations to signal <b>partial success</b> explicitly.
     * Keys left in the {@code keys} collection after an exception will be retried
     * individually; removed keys will not be passed to subsequent calls.
     * <p>
     * Batch deletion does not imply atomicity or ordering guarantees.
     *
     * @param keys the keys of the entries to delete
     */
    void deleteAll(Collection<K> keys);
}

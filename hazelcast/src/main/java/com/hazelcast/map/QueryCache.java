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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A concurrent, queryable data structure which is used to cache results of
 * a continuous query executed on an {@code IMap}. It can be also thought
 * of as an always up to date view or snapshot of the {@code IMap}.
 *
 * Typically, {@code QueryCache} is used for performance reasons.
 *
 * This {@code QueryCache} can be configured via {@link
 * com.hazelcast.config.QueryCacheConfig QueryCacheConfig}.
 *
 * It can be reached like this:
 * <pre>
 * <code>
 *
 *     IMap map = hzInstance.getMap("mapName");
 *     Predicate predicate = TruePredicate.INSTANCE;
 *     QueryCache cache = map.getQueryCache(cacheId, predicate, includeValue);
 *
 * </code>
 * </pre>
 * <p>
 * This cache is evictable. The eviction can be configured with {@link
 * com.hazelcast.config.QueryCacheConfig#setEvictionConfig}.
 * Events caused by {@code IMap} eviction are not reflected to this cache.
 * But the events published after an explicit call to {@link
 * IMap#evict} are reflected to this cache.
 * <p>
 * <b>GOTCHAS</b>
 * <ul>
 * <li>
 * This {@code QueryCache} implementation relies on the eventing system, if
 * a listener is attached to this {@code QueryCache} it may receive same
 * event more than once in case of a system failure. Check out {@link
 * QueryCache#tryRecover()}
 * </li>
 * <li>
 * All writes to this {@link QueryCache} is reflected to underlying {@code
 * IMap} and that write operation will eventually be reflected to this
 * {@code QueryCache} after receiving the event of that operation.
 * </li>
 * <li>
 * Currently, updates performed on the entries are reflected in the indexes
 * in a non-atomic way. Therefore, if there are indexes configured for the
 * query cache, their state may slightly lag behind the state of the
 * entries. Use map listeners if you need to observe the state when the
 * entry store and its indexes are consistent about the state of a
 * particular entry, see {@link IMap#addEntryListener(MapListener, boolean)
 * addEntryListener} for more details.
 * </li>
 * <li>
 * There are some gotchas same with underlying {@link
 * IMap IMap} implementation, one should take care of
 * them before using this {@code QueryCache}. Please check gotchas section
 * in {@link IMap IMap} class for them.
 * </li>
 * </ul>
 * <p>
 *
 * @param <K> the type of key for this {@code QueryCache}
 * @param <V> the type of value for this {@code QueryCache}
 * @see com.hazelcast.config.QueryCacheConfig
 * @since 3.5
 */
public interface QueryCache<K, V> {

    /**
     * @see IMap#get(Object)
     */
    V get(Object key);

    /**
     * @see IMap#containsKey(Object)
     */
    boolean containsKey(Object key);

    /**
     * @see IMap#containsValue(Object)
     */
    boolean containsValue(Object value);

    /**
     * @see IMap#isEmpty()
     */
    boolean isEmpty();

    /**
     * @see IMap#size()
     */
    int size();

    /**
     * @see IMap#addIndex(IndexType, String...)
     */
    default void addIndex(IndexType type, String... attributes) {
        IndexConfig config = IndexUtils.createIndexConfig(type, attributes);

        addIndex(config);
    }

    /**
     * @see IMap#addIndex(IndexConfig)
     */
    void addIndex(IndexConfig config);

    /**
     * @see IMap#getAll(Set)
     */
    Map<K, V> getAll(Set<K> keys);

    /**
     * @see IMap#keySet()
     */
    Set<K> keySet();

    /**
     * @see IMap#values()
     */
    Collection<V> values();

    /**
     * @see IMap#entrySet()
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * @see IMap#keySet(Predicate)
     */
    Set<K> keySet(Predicate<K, V> predicate);

    /**
     * @see IMap#values(Predicate)
     */
    Collection<V> values(Predicate<K, V> predicate);

    /**
     * @see IMap#entrySet(Predicate)
     */
    Set<Map.Entry<K, V>> entrySet(Predicate<K, V> predicate);

    /**
     * @see IMap#addEntryListener(MapListener, boolean)
     */
    UUID addEntryListener(MapListener listener, boolean includeValue);

    /**
     * @see IMap#addEntryListener(MapListener, Object, boolean)
     */
    UUID addEntryListener(MapListener listener, K key, boolean includeValue);

    /**
     * @see IMap#addEntryListener(MapListener, Predicate, boolean)
     */
    UUID addEntryListener(MapListener listener,
                            Predicate<K, V> predicate,
                            boolean includeValue);

    /**
     * @see IMap#addEntryListener(MapListener, Predicate, Object, boolean)
     */
    UUID addEntryListener(MapListener listener,
                            Predicate<K, V> predicate,
                            K key,
                            boolean includeValue);

    /**
     * @see IMap#removeEntryListener(UUID)
     */
    boolean removeEntryListener(UUID id);

    /**
     * Returns the name of this {@code QueryCache}. The returned value will never be null.
     *
     * @return the name of this {@code QueryCache}.
     */
    String getName();

    /**
     * This method can be used to recover from a possible event loss situation. You can detect event loss
     * via {@link com.hazelcast.map.listener.EventLostListener}
     * <p>
     * This method tries to make consistent the data in this {@code QueryCache} with the data in the underlying {@code IMap}
     * by replaying the events after last consistently received ones. As a result of this replaying logic, same event may
     * appear more than once to the {@code QueryCache} listeners.
     * <p>
     * This method returns {@code false} if the event is not in the buffer of event publisher side. That means recovery is not
     * possible.
     *
     * @return {@code true} if the {@code QueryCache} content will be eventually consistent, otherwise {@code false}.
     * @see com.hazelcast.config.QueryCacheConfig#bufferSize
     */
    boolean tryRecover();

    /**
     * Destroys this cache.
     * Clears and releases all local and remote resources created for this cache.
     */
    void destroy();
}



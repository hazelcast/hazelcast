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

package com.hazelcast.replicatedmap.impl.record;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is meant to encapsulate the actual storage system and support automatic waiting for finishing load operations if
 * configured in the {@link com.hazelcast.config.ReplicatedMapConfig}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class InternalReplicatedMapStorage<K, V> {

    private final ConcurrentMap<K, ReplicatedRecord<K, V>> storage =
            new ConcurrentHashMap<>(1000, 0.75f, 1);

    private long version;

    private boolean stale;

    public InternalReplicatedMapStorage() {
    }

    public long getVersion() {
        return version;
    }

    public void syncVersion(long version) {
        this.stale = false;
        this.version = version;
    }

    public void setVersion(long version) {
        if (!stale) {
            stale = (version != (this.version + 1));
        }
        this.version = version;
    }

    public long incrementVersion() {
        return version++;
    }

    public ReplicatedRecord<K, V> get(Object key) {
        return storage.get(key);
    }

    public ReplicatedRecord<K, V> put(K key, ReplicatedRecord<K, V> replicatedRecord) {
        return storage.put(key, replicatedRecord);
    }

    public boolean remove(K key, ReplicatedRecord<K, V> replicatedRecord) {
        return storage.remove(key, replicatedRecord);
    }

    public boolean containsKey(Object key) {
        return storage.containsKey(key);
    }

    public Set<Map.Entry<K, ReplicatedRecord<K, V>>> entrySet() {
        return storage.entrySet();
    }

    public Collection<ReplicatedRecord<K, V>> values() {
        return storage.values();
    }

    public Set<K> keySet() {
        return storage.keySet();
    }

    public void clear() {
        storage.clear();
    }

    public boolean isEmpty() {
        return storage.isEmpty();
    }

    public int size() {
        return storage.size();
    }

    public boolean isStale(long version) {
        return stale || version > this.version;
    }
}

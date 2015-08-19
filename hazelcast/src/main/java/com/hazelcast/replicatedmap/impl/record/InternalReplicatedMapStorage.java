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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.config.ReplicatedMapConfig;
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

    private final ConcurrentMap<K, ReplicatedRecord<K, V>> storage = new ConcurrentHashMap<K, ReplicatedRecord<K, V>>();
    private final ReplicatedMapConfig replicatedMapConfig;
    private final String name;
    private long version;

    InternalReplicatedMapStorage(ReplicatedMapConfig replicatedMapConfig) {
        this.name = replicatedMapConfig.getName();
        this.replicatedMapConfig = replicatedMapConfig;
    }

    public long getPartitionVersion() {
        return version;
    }

    public ReplicatedRecord<K, V> get(Object key) {
        return storage.get(key);
    }

    public ReplicatedRecord<K, V> put(K key, ReplicatedRecord<K, V> replicatedRecord) {
        version++;
        return storage.put(key, replicatedRecord);
    }

    public boolean remove(K key, ReplicatedRecord<K, V> replicatedRecord) {
        version++;
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
        version++;
        storage.clear();
    }

    public boolean isEmpty() {
        return storage.isEmpty();
    }

    public int size() {
        int count = 0;
        for (ReplicatedRecord<K, V> record : storage.values()) {
            if (record.isTombstone()) {
                continue;
            }
            count++;
        }
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalReplicatedMapStorage that = (InternalReplicatedMapStorage) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (!storage.equals(that.storage)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = storage.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}

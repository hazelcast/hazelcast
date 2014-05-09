/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.record;

import com.hazelcast.config.ReplicatedMapConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is meant to encapsulate the actual storage system and support automatic waiting for finishing load operations if
 * configured in the {@link com.hazelcast.config.ReplicatedMapConfig}
 *
 * @param <K> key type
 * @param <V> value type
 */
class InternalReplicatedMapStorage<K, V> {

    private final ConcurrentMap<K, ReplicatedRecord<K, V>> storage = new ConcurrentHashMap<K, ReplicatedRecord<K, V>>();

    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final Lock waitForLoadedLock = new ReentrantLock();
    private final Condition waitForLoadedCondition = waitForLoadedLock.newCondition();

    private final ReplicatedMapConfig replicatedMapConfig;
    private final String name;

    InternalReplicatedMapStorage(ReplicatedMapConfig replicatedMapConfig) {
        this.name = replicatedMapConfig.getName();
        this.replicatedMapConfig = replicatedMapConfig;
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

    public void finishLoading() {
        loaded.set(true);
        waitForLoadedLock.lock();
        try {
            waitForLoadedCondition.signalAll();
        } finally {
            waitForLoadedLock.unlock();
        }
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    public void checkState() {
        if (!loaded.get()) {
            if (!replicatedMapConfig.isAsyncFillup()) {
                while (true) {
                    waitForLoadedLock.lock();
                    try {
                        if (!loaded.get()) {
                            waitForLoadedCondition.await();
                        }
                        // If it is a spurious wakeup we restart waiting
                        if (!loaded.get()) {
                            continue;
                        }
                        // Otherwise return here
                        return;
                    } catch (InterruptedException e) {
                        String name = replicatedMapConfig.getName();
                        throw new IllegalStateException("Synchronous loading of ReplicatedMap '" + name + "' failed.", e);
                    } finally {
                        waitForLoadedLock.unlock();
                    }
                }
            }
        }
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

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

package com.hazelcast.cache.impl.nearcache.impl.store;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.config.NearCacheConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class BaseNearCacheRecordStore<K, V, R extends NearCacheRecord>
        extends AbstractNearCacheRecordStore<K, V, R> {

    protected ConcurrentMap<K, R> store = new ConcurrentHashMap<K, R>();

    public BaseNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheContext nearCacheContext) {
        super(nearCacheConfig, nearCacheContext);
    }

    @Override
    protected boolean isAvailable() {
        return store != null;
    }

    @Override
    protected R getRecord(K key) {
        return store.get(key);
    }

    @Override
    protected R putRecord(K key, R record) {
        R oldRecord = store.put(key, record);
        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
        return oldRecord;
    }

    @Override
    protected R removeRecord(K key) {
        R removedRecord =  store.remove(key);
        if (removedRecord != null) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, removedRecord));
        }
        return removedRecord;
    }

    @Override
    protected void clearRecords() {
        store.clear();
    }

    @Override
    protected void destroyStore() {
        clearRecords();
        // Clear reference so GC can collect it
        store = null;
    }

    @Override
    public int size() {
        checkAvailable();

        return store.size();
    }

    @Override
    public void doExpiration() {
        for (Map.Entry<K, R> entry : store.entrySet()) {
            K key = entry.getKey();
            R value = entry.getValue();
            if (isRecordExpired(value)) {
                remove(key);
            }
        }
    }

}

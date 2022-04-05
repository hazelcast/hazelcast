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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.EntryLoader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestEntryLoader implements EntryLoader<String, String> {

    static final String NULL_RETURNING_KEY = "nullReturningKey";

    private final Map<String, Record> records = new ConcurrentHashMap<>();
    private final AtomicInteger loadedEntryCount = new AtomicInteger();
    private final AtomicInteger loadAllCallCount = new AtomicInteger();
    private final AtomicInteger loadCallCount = new AtomicInteger();
    private final Set<String> loadUniqueKeys = ConcurrentHashMap.newKeySet();
    private final AtomicInteger loadKeysCallCount = new AtomicInteger();

    @Override
    public MetadataAwareValue<String> load(String key) {
        loadCallCount.incrementAndGet();
        loadUniqueKeys.add(key);
        if (NULL_RETURNING_KEY.equals(key)) {
            return null;
        }
        Record record = records.get(key);
        if (record == null) {
            return null;
        }
        loadedEntryCount.incrementAndGet();
        if (record.expirationTime == -1) {
            return new MetadataAwareValue<>(record.value);
        } else {
            return new MetadataAwareValue<>(record.value, record.expirationTime);
        }
    }

    @Override
    public Map<String, MetadataAwareValue<String>> loadAll(Collection<String> keys) {
        loadAllCallCount.incrementAndGet();
        loadedEntryCount.addAndGet(keys.size());
        Map<String, MetadataAwareValue<String>> map = new HashMap<>(keys.size());
        for (String key: keys) {
            Record record = records.get(key);
            map.put(key, new MetadataAwareValue<>(record.value, record.expirationTime));
        }
        return map;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        loadKeysCallCount.incrementAndGet();
        return records.keySet();
    }

    public void putExternally(String key, String value, long expirationTime) {
        this.records.put(key, new Record(value, expirationTime));
    }

    public void putExternally(String key, String value, long ttl, TimeUnit unit) {
        putExternally(key, value, System.currentTimeMillis() + unit.toMillis(ttl));
    }

    public void putExternally(String key, String value) {
        this.records.put(key, new Record(value, Long.MAX_VALUE));
    }

    public int getLoadCallCount() {
        return loadCallCount.get();
    }

    public int getLoadUniqueKeysCount() {
        return loadUniqueKeys.size();
    }

    public int getLoadAllCallCount() {
        return loadAllCallCount.get();
    }

    public int getLoadAllKeysCallCount() {
        return loadKeysCallCount.get();
    }

    public int getLoadedEntryCount() {
        return loadedEntryCount.get();
    }

    private class Record {
        private String value;
        private long expirationTime;

        private Record(String value, long expirationTime) {
            this.value = value;
            this.expirationTime = expirationTime;
        }
    }
}

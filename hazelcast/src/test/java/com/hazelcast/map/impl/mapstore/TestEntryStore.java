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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.EntryLoaderEntry;
import com.hazelcast.map.EntryStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestEntryStore implements EntryStore<String, String> {

    static final String NULL_RETURNING_KEY = "nullReturningKey";

    private Map<String, Record> records = new HashMap<>();
    private AtomicInteger loadedEntryCount = new AtomicInteger();
    private AtomicInteger loadAllCallCount = new AtomicInteger();
    private AtomicInteger loadCallCount = new AtomicInteger();
    private AtomicInteger loadKeysCallCount = new AtomicInteger();
    private AtomicInteger storedEntryCount = new AtomicInteger();
    private AtomicInteger storeCallCount = new AtomicInteger();
    private AtomicInteger storeAllCallCount = new AtomicInteger();
    private AtomicInteger deleteCallCount = new AtomicInteger();

    @Override
    public EntryLoaderEntry<String> load(String key) {
        loadCallCount.incrementAndGet();
        if (NULL_RETURNING_KEY.equals(key)) {
            return null;
        }
        Record record = records.get(key);
        if (record == null) {
            return null;
        }
        loadedEntryCount.incrementAndGet();
        if (record.expirationTime == -1) {
            return new EntryLoaderEntry<>(record.value);
        } else {
            return new EntryLoaderEntry<>(record.value, record.expirationTime);
        }
    }

    @Override
    public Map<String, EntryLoaderEntry<String>> loadAll(Collection<String> keys) {
        loadAllCallCount.incrementAndGet();
        loadedEntryCount.addAndGet(keys.size());
        Map<String, EntryLoaderEntry<String>> map = new HashMap<>(keys.size());
        for (String key: keys) {
            Record record = records.get(key);
            map.put(key, new EntryLoaderEntry<>(record.value, record.expirationTime));
        }
        return map;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        loadKeysCallCount.incrementAndGet();
        return records.keySet();
    }

    @Override
    public void store(String key, EntryLoaderEntry<String> value) {
        String internalValue = value.getValue();
        long expirationTime = value.getExpirationTime();
        if (expirationTime == EntryLoaderEntry.NO_TIME_SET) {
            records.put(key, new Record(internalValue, -1));
        } else {
            records.put(key, new Record(internalValue, expirationTime));
        }
        storeCallCount.incrementAndGet();
        storedEntryCount.incrementAndGet();
    }

    @Override
    public void storeAll(Map<String, EntryLoaderEntry<String>> map) {
        for (Map.Entry<String, EntryLoaderEntry<String>> mapEntry: map.entrySet()) {
            String key = mapEntry.getKey();
            EntryLoaderEntry<String> entry = mapEntry.getValue();
            String internalValue = entry.getValue();
            long expirationTime = entry.getExpirationTime();
            if (expirationTime == EntryLoaderEntry.NO_TIME_SET) {
                records.put(key, new Record(internalValue, -1));
            } else {
                records.put(key, new Record(internalValue, expirationTime));
            }
        }
        storedEntryCount.addAndGet(map.size());
        storeAllCallCount.incrementAndGet();
    }

    @Override
    public void delete(String key) {
        records.remove(key);
        deleteCallCount.incrementAndGet();
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for (String key: keys) {
            records.remove(key);
        }
    }

    public void putExternally(String key, String value, long expirationTime) {
        this.records.put(key, new Record(value, expirationTime));
    }

    public void putExternally(String key, String value) {
        this.records.put(key, new Record(value, Long.MAX_VALUE));
    }

    public int getLoadCallCount() {
        return loadCallCount.get();
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

    public int getLoadKeysCallCount() {
        return loadKeysCallCount.get();
    }

    public int getStoredEntryCount() {
        return storedEntryCount.get();
    }

    public int getStoreCallCount() {
        return storeCallCount.get();
    }

    public int getStoreAllCallCount() {
        return storeAllCallCount.get();
    }

    public int getDeleteCallCount() {
        return deleteCallCount.get();
    }

    public String getExternal(String key) {
        Record record = records.get(key);
        if (record == null) {
            return null;
        }
        return record.value;
    }

    public Record getRecord(String key) {
        return records.get(key);
    }

    class Record {
        String value;
        long expirationTime;

        private Record(String value, long expirationTime) {
            this.value = value;
            this.expirationTime = expirationTime;
        }
    }
}

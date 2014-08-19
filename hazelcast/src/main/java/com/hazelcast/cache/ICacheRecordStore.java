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

package com.hazelcast.cache;

import com.hazelcast.cache.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.serialization.Data;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Map;
import java.util.Set;

public interface ICacheRecordStore {

    Object get(Data key, ExpiryPolicy expiryPolicy);

    void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller);

    Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller);

    boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller);

    Object getAndRemove(Data key, String caller);

    boolean remove(Data key, String caller);

    boolean remove(Data key, Object value, String caller);

    boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller);

    boolean replace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy, String caller);

    Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller);

    boolean contains(Data key);

    MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy);

    int size();

    void clear(Set<Data> keys, boolean isRemoveAll);

    void destroy();

//    boolean hasExpiringEntry();
//
//    Callback<Data> createEvictionCallback();

    CacheConfig getConfig();

    String getName();

    Map<Data, CacheRecord> getReadOnlyRecords();

    CacheRecord getRecord(Data key);

    void setRecord(Data key, CacheRecord record);

    CacheRecord removeRecord(Data key);

    CacheKeyIteratorResult iterator(int tableIndex, int size);

    Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments);

    Set<Data> loadAll(Set<Data> keys, boolean replaceExistingValues);

//    void setRecordStoreMode(boolean storeOrBackup);

    CacheStatistics getCacheStats();

}

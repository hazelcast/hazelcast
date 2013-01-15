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

package com.hazelcast.map;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public interface RecordStore {

    boolean tryRemove(Data dataKey);

    Data remove(Data dataKey);

    boolean remove(Data dataKey, Data testValue);

    Data get(Data dataKey);

    Data put(Data dataKey, Data dataValue, long ttl);

    void put(Map.Entry<Data, Data> entry);

    Data replace(Data dataKey, Data value);

    boolean replace(Data dataKey, Data oldValue, Data newValue);

    void set(Data dataKey, Data value, long ttl);

    void putTransient(Data dataKey, Data value, long ttl);

    boolean tryPut(Data dataKey, Data value, long ttl);

    Data putIfAbsent(Data dataKey, Data value, long ttl);

    ConcurrentMap<Data, Record> getRecords();

    Set<Data> keySet();

    int size();

    boolean forceUnlock(Data key);

    boolean isLocked(Data key);

    boolean lock(Data key, Address caller, int threadId, long ttl);

    boolean containsValue(Data testValue);

    LockInfo getOrCreateLock(Data key);

    boolean canRun(LockAwareOperation lockAwareOperation);

    LockInfo getLock(Data key);

    boolean evict(Data key);

    boolean unlock(Data key, Address caller, int threadId);

    Collection<Data> values();

    MapInfo getMapInfo();

    Set<Data> getRemovedDelayedKeys();

    Set<Map.Entry<Data, Data>> entrySet();

    Map.Entry<Data,Data> getMapEntry(Data dataKey);

    void setRecordValue(Record record, Object value);
}

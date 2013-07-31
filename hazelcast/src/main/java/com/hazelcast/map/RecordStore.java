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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface RecordStore {

    Object remove(Data dataKey);

    boolean remove(Data dataKey, Object testValue);

    Object get(Data dataKey);

    boolean containsKey(Data dataKey);

    Object put(Data dataKey, Object dataValue, long ttl);

    void put(Map.Entry<Data, Object> entry);

    Object replace(Data dataKey, Object value);

    boolean replace(Data dataKey, Object oldValue, Object newValue);

    boolean set(Data dataKey, Object value, long ttl);

    void putTransient(Data dataKey, Object value, long ttl);

    boolean tryPut(Data dataKey, Object value, long ttl);

    Object putIfAbsent(Data dataKey, Object value, long ttl);

    boolean merge(Data dataKey, EntryView mergingEntryView, MapMergePolicy mergePolicy);

    Map<Data, Record> getRecords();

    Set<Data> keySet();

    int size();

    boolean lock(Data key, String caller, int threadId, long ttl);

    boolean txnLock(Data key, String caller, int threadId, long ttl);

    boolean extendLock(Data key, String caller, int threadId, long ttl);

    boolean unlock(Data key, String caller, int threadId);

    boolean isLocked(Data key);

    boolean isLockedBy(Data key, String caller, int threadId);

    boolean canAcquireLock(Data key, String caller, int threadId);

    String getLockOwnerInfo(Data key);

    boolean containsValue(Object testValue);

    Object evict(Data key);

    Collection<Object> valuesObject();

    Collection<Data> valuesData();

    MapContainer getMapContainer();

    Set<Map.Entry<Data, Object>> entrySetObject();

    Set<Map.Entry<Data, Data>> entrySetData();

    Map.Entry<Data,Data> getMapEntryData(Data dataKey);

    Map.Entry<Data,Object> getMapEntryObject(Data dataKey);

    void flush();

    void removeAll();

    void reset();

    boolean forceUnlock(Data dataKey);
}

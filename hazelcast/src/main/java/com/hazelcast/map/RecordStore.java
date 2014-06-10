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

    String getName();

    Object remove(Data dataKey);

    boolean remove(Data dataKey, Object testValue);

    Object get(Data dataKey);

    MapEntrySet getAll(Set<Data> keySet);

    boolean containsKey(Data dataKey);

    Object put(Data dataKey, Object dataValue, long ttl);

    void put(Map.Entry<Data, Object> entry);

    Record putBackup(Data key, Object value);

    Record putBackup(Data key, Object value, long ttl, boolean shouldSchedule);

    Object replace(Data dataKey, Object value);

    boolean replace(Data dataKey, Object oldValue, Object newValue);

    boolean set(Data dataKey, Object value, long ttl);

    void putTransient(Data dataKey, Object value, long ttl);

    void putFromLoad(Data dataKey, Object value, long ttl);

    boolean tryPut(Data dataKey, Object value, long ttl);

    Object putIfAbsent(Data dataKey, Object value, long ttl);

    boolean merge(Data dataKey, EntryView mergingEntryView, MapMergePolicy mergePolicy);

    Record getRecord(Data key);

    Record putRecord(Data key, Record record);

    void deleteRecord(Data key);

    Map<Data, Record> getReadonlyRecordMap();

    Set<Data> keySet();

    int size();

    boolean lock(Data key, String caller, long threadId, long ttl);

    boolean txnLock(Data key, String caller, long threadId, long ttl);

    boolean extendLock(Data key, String caller, long threadId, long ttl);

    boolean unlock(Data key, String caller, long threadId);

    boolean isLocked(Data key);

    boolean isLockedBy(Data key, String caller, long threadId);

    boolean canAcquireLock(Data key, String caller, long threadId);

    String getLockOwnerInfo(Data key);

    boolean containsValue(Object testValue);

    Object evict(Data key);

    Collection<Object> valuesObject();

    Collection<Data> valuesData();

    MapContainer getMapContainer();

    Set<Map.Entry<Data, Object>> entrySetObject();

    Set<Map.Entry<Data, Data>> entrySetData();

    Map.Entry<Data, Object> getMapEntry(Data dataKey);

    Map.Entry<Data, Object> getMapEntryForBackup(Data dataKey);

    void flush();

    void clearPartition();

    void reset();

    boolean forceUnlock(Data dataKey);

    long getHeapCost();

    SizeEstimator getSizeEstimator();

    boolean isLoaded();

    void checkIfLoaded();

    void setLoaded(boolean loaded);

    void clear();

    boolean isEmpty();
}

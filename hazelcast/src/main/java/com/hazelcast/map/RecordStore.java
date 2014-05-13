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
import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.map.writebehind.WriteBehindQueue;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RecordStore {

    String getName();

    Object put(Data dataKey, Object dataValue, long ttl);

    void put(Map.Entry<Data, Object> entry);

    Object putIfAbsent(Data dataKey, Object value, long ttl);

    Record putBackup(Data key, Object value);

    Record putBackup(Data key, Object value, long ttl);

    boolean tryPut(Data dataKey, Object value, long ttl);

    boolean set(Data dataKey, Object value, long ttl);

    Object remove(Data dataKey);

    boolean remove(Data dataKey, Object testValue);

    /**
     * Similar to {@link com.hazelcast.map.RecordStore#remove(com.hazelcast.nio.serialization.Data)}
     * except removeBackup doesn't touch mapstore since it does not return previous value.
     */
    void removeBackup(Data dataKey);

    Object get(Data dataKey);

    MapEntrySet getAll(Set<Data> keySet);

    boolean containsKey(Data dataKey);

    Object replace(Data dataKey, Object value);

    boolean replace(Data dataKey, Object oldValue, Object newValue);

    void putTransient(Data dataKey, Object value, long ttl);

    void putFromLoad(Data dataKey, Object value, long ttl);

    boolean merge(Data dataKey, EntryView mergingEntryView, MapMergePolicy mergePolicy);

    Record getRecord(Data key);

    void putForReplication(Data key, Record record);

    void deleteRecord(Data key);

    Map<Data, Record> getReadonlyRecordMap();

    Set<Data> keySet();

    int size();

    boolean lock(Data key, String caller, long threadId, long ttl);

    boolean txnLock(Data key, String caller, long threadId, long ttl);

    boolean extendLock(Data key, String caller, long threadId, long ttl);

    boolean unlock(Data key, String caller, long threadId);

    boolean isLocked(Data key);

    boolean canAcquireLock(Data key, String caller, long threadId);

    String getLockOwnerInfo(Data key);

    boolean containsValue(Object testValue);

    Object evict(Data key);

    Collection<Data> valuesData();

    MapContainer getMapContainer();

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

    WriteBehindQueue<DelayedEntry> getWriteBehindQueue();

    List clearUnLockedExpiredRecords();

    void removeFromWriteBehindWaitingDeletions(Data key);
}

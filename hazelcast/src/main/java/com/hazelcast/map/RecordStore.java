/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.impl.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public interface RecordStore {

    boolean tryRemove(Data dataKey);

    Data remove(Data dataKey);

    boolean remove(Data dataKey, Data testValue);

    Data get(Data dataKey);

    Data put(Data dataKey, Data dataValue, long ttl);

    Data replace(Data dataKey, Data dataValue);

    boolean replace(Data dataKey, Data oldValue, Data newValue);

    void set(Data dataKey, Data dataValue, long ttl);

    void putTransient(Data dataKey, Data dataValue, long ttl);

    boolean tryPut(Data dataKey, Data dataValue, long ttl);

    Data putIfAbsent(Data dataKey, Data dataValue, long ttl);

    ConcurrentMap<Data, Record> getRecords();

    void removeAll(List<Data> keys);

    Set<Data> keySet();

    int size();

    boolean forceUnlock(Data dataKey);

    boolean isLocked(Data dataKey);

    boolean lock(Data key, Address caller, int threadId, long ttl);

    int getBackupCount();

    int getAsyncBackupCount();

    boolean containsValue(Data testValue);

    LockInfo getOrCreateLock(Data key);

    boolean canRun(LockAwareOperation lockAwareOperation);

    LockInfo getLock(Data key);

    boolean evict(Data dataKey);

    boolean unlock(Data dataKey, Address caller, int threadId);

    Collection<Data> values();
}

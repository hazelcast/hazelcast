/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.impl.Record;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.nio.Data;

import java.util.HashMap;
import java.util.Map;

public class MapPartition {
    final String name;
    final PartitionInfo partitionInfo;
    final PartitionContainer partitionContainer;
    final Map<Data, Record> records = new HashMap<Data, Record>(1000);
    final Map<Data, LockInfo> locks = new HashMap<Data, LockInfo>(100);
    final MapLoader loader;
    final MapStore store;
    final long writeDelayMillis = 0;
    final int backupCount;

    public MapPartition(String name, PartitionContainer partitionContainer) {
        this.name = name;
        this.partitionInfo = partitionContainer.partitionInfo;
        this.partitionContainer = partitionContainer;
        loader = null;
        store = null;
        backupCount = partitionContainer.getMapConfig(name).getBackupCount();
    }

    public LockInfo getOrCreateLock(Data key) {
        LockInfo lock = locks.get(key);
        if (lock == null) {
            lock = new LockInfo();
            locks.put(key, lock);
        }
        return lock;
    }

    public LockInfo getLock(Data key) {
        return locks.get(key);
    }

    void removeLock(Data key) {
        locks.remove(key);
    }

    public int getBackupCount() {
        return backupCount;
    }

    boolean canRun(LockAwareOperation op) {
        LockInfo lock = locks.get(op.getKey());
        return lock == null || lock.testLock(op.getThreadId(), op.getCaller());
    }

    void clear() {
        records.clear();
        locks.clear();
    }
}

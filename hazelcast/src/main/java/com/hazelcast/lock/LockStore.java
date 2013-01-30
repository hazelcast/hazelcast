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

package com.hazelcast.lock;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockStore {

    private final ConcurrencyUtil.ConstructorFunction<Data,LockInfo> lockConstructor
            = new ConcurrencyUtil.ConstructorFunction<Data,LockInfo>() {
        public LockInfo createNew(Data key) {
            return new LockInfo();
        }
    };


    private final ConcurrentMap<Data, LockInfo> locks = new ConcurrentHashMap<Data, LockInfo>();

    public LockInfo getLock(Data key) {
        return locks.get(key);
    }

    public LockInfo getOrCreateLock(Data key) {
        return ConcurrencyUtil.getOrPutIfAbsent(locks, key, lockConstructor);
    }

    public void putLock(Data key, LockInfo lock) {
        locks.put(key, lock);
    }

    public boolean lock(Data key, Address caller, int threadId) {
        return lock(key, caller, threadId, Long.MAX_VALUE);
    }

    public boolean lock(Data key, Address caller, int threadId, long ttl) {
        final LockInfo lock = getOrCreateLock(key);
        return lock.lock(caller, threadId, ttl);
    }

    public boolean isLocked(Data key) {
        final LockInfo lock = locks.get(key);
        return lock != null && lock.isLocked();
    }

    public boolean canAcquireLock(Data key, Address caller, int threadId) {
        final LockInfo lock = locks.get(key);
        return lock == null || lock.canAcquireLock(caller, threadId);
    }

    public boolean unlock(Data key, Address caller, int threadId) {
        final LockInfo lock = locks.get(key);
        boolean result = false;
        if (lock == null)
            return result;
        if (lock.canAcquireLock(caller, threadId)) {
            if (lock.unlock(caller, threadId)) {
                result = true;
            }
        }
        if (!lock.isLocked()) {
            locks.remove(key);
        }
        return result;
    }

    public boolean forceUnlock(Data key) {
        final LockInfo lock = getLock(key);
        if (lock == null)
            return false;
        lock.clear();
        return true;
    }

    public Map<Data,LockInfo> getLocks() {
        return Collections.unmodifiableMap(locks);
    }

    public void clear() {
        locks.clear();
    }
}

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

package com.hazelcast.lock;

import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalLockStats;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * @ali 1/21/13
 */
public class ObjectLockProxy implements ILock {

    public static final String LOCK_MAP_NAME = "hz:map:lock";

    final NodeEngine nodeEngine;

    final Object key;

    final IMap<Object, Object> lockMap;

    public ObjectLockProxy(NodeEngine nodeEngine, Object key, IMap<Object, Object> lockMap) {
        this.nodeEngine = nodeEngine;
        this.key = key;
        this.lockMap = lockMap;
    }

    public Object getLockObject() {
        return key;
    }

    public LocalLockStats getLocalLockStats() {
        return null;
    }

    public boolean isLocked() {
        return lockMap.isLocked(key);
    }

    public void forceUnlock() {
        lockMap.forceUnlock(key);
    }

    public Object getId() {
        return key;
    }

    public String getName() {
        return String.valueOf(key);
    }

    public void destroy() {
    }

    public void lock() {
        lockMap.lock(key);
    }

    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public boolean tryLock() {
        return lockMap.tryLock(key);
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lockMap.tryLock(key, time, unit);
    }

    public void unlock() {
        lockMap.unlock(key);
    }

    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}

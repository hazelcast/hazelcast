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

    final String name;

    final IMap lockMap;

    final Object lockObject;

    public ObjectLockProxy(NodeEngine nodeEngine, String name, IMap lockMap) {
        this.nodeEngine = nodeEngine;
        this.name = name;
        this.lockMap = lockMap;
        this.lockObject = new Object();
    }

    public Object getLockObject() {
        return lockObject;
    }

    public LocalLockStats getLocalLockStats() {
        return null;
    }

    public boolean isLocked() {
        return lockMap.isLocked(name);
    }

    public void forceUnlock() {
        lockMap.forceUnlock(name);
    }

    public Object getId() {
        return name;//TODO
    }

    public String getName() {
        return name;
    }

    public void destroy() {

    }

    public void lock() {
        lockMap.lock(name);
    }

    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public boolean tryLock() {
        return lockMap.tryLock(name);
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lockMap.tryLock(name, time, unit);
    }

    public void unlock() {
        lockMap.unlock(name);
    }

    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}

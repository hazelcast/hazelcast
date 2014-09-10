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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

import java.util.Set;

public final class LockStoreProxy implements LockStore {

    private final LockStoreContainer container;
    private final ObjectNamespace namespace;

    public LockStoreProxy(LockStoreContainer container, ObjectNamespace namespace) {
        this.container = container;
        this.namespace = namespace;
    }

    @Override
    public boolean lock(Data key, String caller, long threadId, long ttl) {
        LockStore lockStore = getLockStore();
        return lockStore.lock(key, caller, threadId, ttl);
    }

    @Override
    public boolean txnLock(Data key, String caller, long threadId, long ttl) {
        LockStore lockStore = getLockStore();
        return lockStore.txnLock(key, caller, threadId, ttl);
    }

    @Override
    public boolean extendLeaseTime(Data key, String caller, long threadId, long ttl) {
        LockStore lockStore = getLockStore();
        return lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    @Override
    public boolean unlock(Data key, String caller, long threadId) {
        LockStore lockStore = getLockStore();
        return lockStore.unlock(key, caller, threadId);
    }

    @Override
    public boolean isLocked(Data key) {
        LockStore lockStore = getLockStore();
        return lockStore.isLocked(key);
    }

    @Override
    public boolean isLockedBy(Data key, String caller, long threadId) {
        LockStore lockStore = getLockStore();
        return lockStore.isLockedBy(key, caller, threadId);
    }

    @Override
    public int getLockCount(Data key) {
        LockStore lockStore = getLockStore();
        return lockStore.getLockCount(key);
    }

    @Override
    public long getRemainingLeaseTime(Data key) {
        LockStore lockStore = getLockStore();
        return lockStore.getRemainingLeaseTime(key);
    }

    @Override
    public boolean canAcquireLock(Data key, String caller, long threadId) {
        LockStore lockStore = getLockStore();
        return lockStore.canAcquireLock(key, caller, threadId);
    }

    @Override
    public boolean isTransactionallyLocked(Data key) {
        LockStore lockStore = getLockStore();
        return lockStore.isTransactionallyLocked(key);
    }

    @Override
    public Set<Data> getLockedKeys() {
        LockStore lockStore = getLockStore();
        return lockStore.getLockedKeys();
    }

    @Override
    public boolean forceUnlock(Data key) {
        LockStore lockStore = getLockStore();
        return lockStore.forceUnlock(key);
    }

    @Override
    public String getOwnerInfo(Data dataKey) {
        LockStore lockStore = getLockStore();
        return lockStore.getOwnerInfo(dataKey);
    }

    private LockStore getLockStore() {
        return container.getLockStore(namespace);
    }
}

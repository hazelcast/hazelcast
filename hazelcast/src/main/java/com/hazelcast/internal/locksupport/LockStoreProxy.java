/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Proxy to a {@link LockStoreImpl}. Since a {@link LockStoreImpl} may be destroyed
 * upon service reset (eg. after a split-brain is healed), some of this proxy's methods
 * employ get-or-create to locate the {@link LockStoreImpl} or get-or-null when
 * creation of the {@link LockStoreImpl} is not required and its absence is
 * interpreted as "no lock exists".
 */
public final class LockStoreProxy implements LockStore {

    static final String NOT_LOCKED = "<not-locked>";

    private final LockStoreContainer container;
    private final ObjectNamespace namespace;

    public LockStoreProxy(LockStoreContainer container, ObjectNamespace namespace) {
        this.container = container;
        this.namespace = namespace;
    }

    @Override
    public boolean lock(Data key, UUID caller, long threadId, long referenceId, long leaseTime) {
        LockStore lockStore = getOrCreateLockStore();
        return lockStore != null && lockStore.lock(key, caller, threadId, referenceId, leaseTime);
    }

    @Override
    public boolean localLock(Data key, UUID caller, long threadId, long referenceId, long leaseTime) {
        LockStore lockStore = getOrCreateLockStore();
        return lockStore != null && lockStore.localLock(key, caller, threadId, referenceId, leaseTime);
    }

    @Override
    public boolean txnLock(Data key, UUID caller, long threadId, long referenceId, long leaseTime, boolean blockReads) {
        LockStore lockStore = getOrCreateLockStore();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, referenceId, leaseTime, blockReads);
    }

    @Override
    public boolean extendLeaseTime(Data key, UUID caller, long threadId, long leaseTime) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, leaseTime);
    }

    @Override
    public boolean unlock(Data key, UUID caller, long threadId, long referenceId) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore != null && lockStore.unlock(key, caller, threadId, referenceId);
    }

    @Override
    public boolean isLocked(Data key) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore != null && lockStore.isLocked(key);
    }

    @Override
    public boolean isLockedBy(Data key, UUID caller, long threadId) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore != null && lockStore.isLockedBy(key, caller, threadId);
    }

    @Override
    public int getLockCount(Data key) {
        LockStore lockStore = getLockStoreOrNull();
        if (lockStore == null) {
            return 0;
        }
        return lockStore.getLockCount(key);
    }

    @Override
    public int getLockedEntryCount() {
        LockStore lockStore = getLockStoreOrNull();
        if (lockStore == null) {
            return 0;
        }
        return lockStore.getLockedEntryCount();
    }

    @Override
    public long getRemainingLeaseTime(Data key) {
        LockStore lockStore = getLockStoreOrNull();
        if (lockStore == null) {
            return 0;
        }
        return lockStore.getRemainingLeaseTime(key);
    }

    @Override
    public boolean canAcquireLock(Data key, UUID caller, long threadId) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    @Override
    public boolean shouldBlockReads(Data key) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore != null && lockStore.shouldBlockReads(key);
    }

    @Override
    public Set<Data> getLockedKeys() {
        LockStore lockStore = getLockStoreOrNull();
        if (lockStore == null) {
            return Collections.emptySet();
        }
        return lockStore.getLockedKeys();
    }

    @Override
    public boolean forceUnlock(Data key) {
        LockStore lockStore = getLockStoreOrNull();
        return lockStore != null && lockStore.forceUnlock(key);
    }

    @Override
    public String getOwnerInfo(Data dataKey) {
        LockStore lockStore = getLockStoreOrNull();
        if (lockStore == null) {
            return NOT_LOCKED;
        }
        return lockStore.getOwnerInfo(dataKey);
    }

    private LockStore getOrCreateLockStore() {
        return container.getOrCreateLockStore(namespace);
    }

    private LockStore getLockStoreOrNull() {
        return container.getLockStore(namespace);
    }
}

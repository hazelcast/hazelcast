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

import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 2/12/13
 */
public class LockStoreContainer {

    private final LockService lockService;
    private final int partitionId;
    private final ConcurrentMap<ObjectNamespace, LockStoreImpl> lockStores = new ConcurrentHashMap<ObjectNamespace, LockStoreImpl>();
    private final ConstructorFunction<ObjectNamespace, LockStoreImpl> lockStoreConstructor
            = new ConstructorFunction<ObjectNamespace, LockStoreImpl>() {
        public LockStoreImpl createNew(ObjectNamespace key) {
            return new LockStoreImpl(key, 1, 0, lockService);
        }
    };

    public LockStoreContainer(LockService lockService, int partitionId) {
        this.lockService = lockService;
        this.partitionId = partitionId;
    }

    public LockStoreImpl createLockStore(ObjectNamespace namespace, int backupCount, int asyncBackupCount) {
        final LockStoreImpl ls = new LockStoreImpl(namespace, backupCount, asyncBackupCount, lockService);
        final LockStoreImpl current;
        if ((current = lockStores.putIfAbsent(namespace, ls)) != null) {
            if (current.getBackupCount() != ls.getBackupCount()
                    || current.getAsyncBackupCount() != ls.getAsyncBackupCount()) {
                throw new IllegalStateException("LockStore for namespace[" + namespace + "] is already created! " +
                        current + " - VS - " + ls);
            }
            return current;
        }
        return ls;
    }

    void clearLockStore(ObjectNamespace namespace) {
        final LockStoreImpl lockStore = lockStores.get(namespace);
        if (lockStore != null) {
            lockStore.clear();
        }
    }

    LockStoreImpl getOrCreateDefaultLockStore(ObjectNamespace namespace) {
        return ConcurrencyUtil.getOrPutIfAbsent(lockStores, namespace, lockStoreConstructor);
    }

    LockStoreImpl getLockStore(ObjectNamespace namespace) {
        return lockStores.get(namespace);
    }

    public Collection<LockStoreImpl> getLockStores() {
        return Collections.unmodifiableCollection(lockStores.values());
    }

    void clear() {
        for (LockStoreImpl lockStore : lockStores.values()) {
            lockStore.clear();
        }
        lockStores.clear();
    }

    int getPartitionId() {
        return partitionId;
    }

    void put(LockStoreImpl ls) {
        Collection<DistributedLock> lockInfos = ls.getLocks().values();
        for (DistributedLock lockInfo : lockInfos) {
            lockInfo.setLockService(lockService);
            lockInfo.setNamespace(ls.getNamespace());
        }
        ls.setLockService(lockService);
        lockStores.put(ls.getNamespace(), ls);
    }
}
